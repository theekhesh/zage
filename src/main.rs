use anyhow::{Context, Result, bail};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode},
    execute,
    terminal::{
        Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
        enable_raw_mode, size,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{Mutex as AsyncMutex, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

const MAX_ACTIVE_DOWNLOADS: usize = 3;
const MAX_SEGMENTS: usize = 8;
const MIN_SEGMENT_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum Status {
    Queued,
    Downloading,
    Paused,
    Complete,
    Failed,
}

fn default_priority() -> u8 {
    1
}

fn adjust_priority(shared: &SharedState, selected: usize, delta: i8) {
    if let Some(download) = shared
        .lock()
        .expect("download state mutex poisoned")
        .downloads
        .get_mut(selected)
    {
        download.priority = if delta > 0 {
            download.priority.saturating_add(1).min(2)
        } else {
            download.priority.saturating_sub(1)
        };
    }
}

fn refresh_speeds(
    shared: &SharedState,
    samples: &mut std::collections::HashMap<u64, (u64, Instant)>,
) {
    let now = Instant::now();
    let mut state = shared.lock().expect("download state mutex poisoned");
    for download in &mut state.downloads {
        let sample = samples
            .entry(download.id)
            .or_insert((download.downloaded, now));
        let elapsed = now.duration_since(sample.1).as_secs_f64();
        download.speed_bps = if elapsed > 0.0 {
            ((download.downloaded.saturating_sub(sample.0)) as f64 / elapsed) as u64
        } else {
            0
        };
        *sample = (download.downloaded, now);
    }
}

impl Status {
    fn label(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Paused => "paused",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Download {
    id: u64,
    url: String,
    output: PathBuf,
    status: Status,
    #[serde(default = "default_priority")]
    priority: u8,
    downloaded: u64,
    total: Option<u64>,
    #[serde(default)]
    speed_bps: u64,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
    error: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
struct AppState {
    next_id: u64,
    downloads: Vec<Download>,
    #[serde(default)]
    speed_limit_bps: Option<u64>,
}

type SharedState = Arc<Mutex<AppState>>;

struct RateLimiter {
    limit_bps: AtomicU64,
    gate: AsyncMutex<()>,
}

impl RateLimiter {
    fn new(limit_bps: Option<u64>) -> Self {
        Self {
            limit_bps: AtomicU64::new(limit_bps.unwrap_or(0)),
            gate: AsyncMutex::new(()),
        }
    }

    fn set_limit(&self, limit_bps: Option<u64>) {
        self.limit_bps
            .store(limit_bps.unwrap_or(0), Ordering::Relaxed);
    }

    async fn acquire(&self, bytes: usize) {
        let _gate = self.gate.lock().await;
        let limit = self.limit_bps.load(Ordering::Relaxed);
        if limit == 0 || bytes == 0 {
            return;
        }
        let duration = Duration::from_secs_f64(bytes as f64 / limit as f64);
        tokio::time::sleep(duration).await;
    }
}

struct Worker {
    pause: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum InputMode {
    Add,
    SpeedLimit,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("--help") || first.as_deref() == Some("-h") {
        print_help();
        return Ok(());
    }

    let state_path = state_path()?;
    let mut state = load_state(&state_path)?;
    for download in &mut state.downloads {
        if download.status == Status::Downloading {
            download.status = Status::Queued;
        }
    }

    if let Some(url) = first {
        let output = args
            .next()
            .map(PathBuf::from)
            .unwrap_or(default_output_path(&url)?);
        add_download(&mut state, url, output);
    }

    let shared = Arc::new(Mutex::new(state));
    save_state(&shared, &state_path)?;
    run_tui(shared, state_path).await
}

fn print_help() {
    println!("Zage — lightweight Linux download manager");
    println!();
    println!("Usage:");
    println!("  zage [URL [OUTPUT]]");
    println!();
    println!("Without arguments, opens the download queue. In the UI:");
    println!("  a  add URL [optional output path]");
    println!("  p  pause selected download");
    println!("  r  resume or retry selected download");
    println!("  d  remove selected queue entry");
    println!("  c  clear completed downloads");
    println!("  s  set global speed limit (e.g. 2M, or off)");
    println!("  +/-  lower or raise queued task priority");
    println!("  j/k or arrows  select download");
    println!("  ? or F1  show keyboard help");
    println!("  q  quit");
}

fn state_path() -> Result<PathBuf> {
    let base = if let Some(path) = env::var_os("XDG_STATE_HOME") {
        PathBuf::from(path)
    } else {
        PathBuf::from(env::var_os("HOME").context("HOME is not set")?).join(".local/state")
    };
    Ok(base.join("zage/downloads.json"))
}

fn load_state(path: &Path) -> Result<AppState> {
    match fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)
            .with_context(|| format!("could not parse {}", path.display()))?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(AppState::default()),
        Err(error) => Err(error).with_context(|| format!("could not read {}", path.display())),
    }
}

fn save_state(state: &SharedState, path: &Path) -> Result<()> {
    let bytes = {
        let state = state.lock().expect("download state mutex poisoned");
        serde_json::to_vec_pretty(&*state)?
    };
    let parent = path
        .parent()
        .context("state path has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("could not create {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, bytes)
        .with_context(|| format!("could not write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| format!("could not save {}", path.display()))?;
    Ok(())
}

fn add_download(state: &mut AppState, url: String, output: PathBuf) {
    let id = state.next_id;
    state.next_id += 1;
    state.downloads.push(Download {
        id,
        url,
        output,
        status: Status::Queued,
        priority: 1,
        downloaded: 0,
        total: None,
        speed_bps: 0,
        etag: None,
        last_modified: None,
        error: None,
    });
}

fn default_output_path(url: &str) -> Result<PathBuf> {
    let parsed = reqwest::Url::parse(url).context("invalid URL")?;
    let filename = parsed
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .unwrap_or("download");
    let name = Path::new(filename)
        .file_name()
        .context("URL does not contain a valid filename")?;
    Ok(PathBuf::from(name))
}

async fn run_tui(shared: SharedState, state_path: PathBuf) -> Result<()> {
    enable_raw_mode().context("could not enable terminal raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Hide)?;

    let result = tui_loop(&shared, &state_path).await;

    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
    result
}

async fn tui_loop(shared: &SharedState, state_path: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("zage/0.1")
        .connect_timeout(Duration::from_secs(15))
        .build()
        .context("could not create HTTP client")?;
    let initial_limit = shared
        .lock()
        .expect("download state mutex poisoned")
        .speed_limit_bps;
    let rate_limiter = Arc::new(RateLimiter::new(initial_limit));
    let mut workers: std::collections::HashMap<u64, Worker> = std::collections::HashMap::new();
    let mut selected = 0_usize;
    let mut input = None::<(InputMode, String)>;
    let mut help_visible = false;
    let mut notice = None::<String>;
    let mut speed_samples = std::collections::HashMap::<u64, (u64, Instant)>::new();
    let mut last_save = Instant::now();
    let mut stdout = io::stdout();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<std::result::Result<Event, String>>();
    let event_reader = tokio::task::spawn_blocking(move || {
        while !event_tx.is_closed() {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(event) => {
                        if event_tx.send(Ok(event)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = event_tx.send(Err(error.to_string()));
                        break;
                    }
                },
                Ok(false) => {}
                Err(error) => {
                    let _ = event_tx.send(Err(error.to_string()));
                    break;
                }
            }
        }
    });

    'ui: loop {
        let finished: Vec<u64> = workers
            .iter()
            .filter_map(|(id, worker)| worker.handle.is_finished().then_some(*id))
            .collect();
        for id in finished {
            if let Some(worker) = workers.remove(&id)
                && let Err(error) = worker.handle.await
            {
                set_failure(shared, id, format!("download task stopped: {error}"));
            }
        }

        let active_count = workers.len();
        let queued: Vec<Download> = {
            let state = shared.lock().expect("download state mutex poisoned");
            let mut queued: Vec<Download> = state
                .downloads
                .iter()
                .filter(|download| {
                    download.status == Status::Queued && !workers.contains_key(&download.id)
                })
                .cloned()
                .collect();
            queued.sort_by_key(|download| std::cmp::Reverse(download.priority));
            queued.truncate(MAX_ACTIVE_DOWNLOADS.saturating_sub(active_count));
            queued
        };
        for download in queued {
            let (pause, receiver) = watch::channel(false);
            let shared = Arc::clone(shared);
            let client = client.clone();
            let rate_limiter = Arc::clone(&rate_limiter);
            let id = download.id;
            let handle = tokio::spawn(async move {
                if let Err(error) =
                    run_download(shared.clone(), client, download, receiver, rate_limiter).await
                {
                    set_failure(&shared, id, format!("{error:#}"));
                }
            });
            workers.insert(id, Worker { pause, handle });
        }

        refresh_speeds(shared, &mut speed_samples);
        draw(
            &mut stdout,
            shared,
            selected,
            input.as_ref(),
            help_visible,
            notice.as_deref(),
        )?;
        if last_save.elapsed() >= Duration::from_secs(1) {
            save_state(shared, state_path)?;
            last_save = Instant::now();
        }

        while let Ok(event) = event_rx.try_recv() {
            let event = event.map_err(anyhow::Error::msg)?;
            if let Event::Key(key) = event {
                if let Some((mode, mut value)) = input.take() {
                    match key.code {
                        KeyCode::Enter => match mode {
                            InputMode::Add => match parse_input(&value) {
                                Ok(Some((url, output))) => {
                                    let mut state =
                                        shared.lock().expect("download state mutex poisoned");
                                    add_download(&mut state, url, output);
                                    notice = Some("Download added to the queue".to_owned());
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    notice = Some(format!("{error:#}"));
                                    input = Some((mode, value));
                                }
                            },
                            InputMode::SpeedLimit => match parse_speed_limit(&value) {
                                Ok(limit) => {
                                    shared
                                        .lock()
                                        .expect("download state mutex poisoned")
                                        .speed_limit_bps = limit;
                                    rate_limiter.set_limit(limit);
                                    notice = Some(match limit {
                                        Some(limit) => {
                                            format!("Speed limit set to {}", human_rate(limit))
                                        }
                                        None => "Speed limit removed".to_owned(),
                                    });
                                }
                                Err(error) => {
                                    notice = Some(format!("{error:#}"));
                                    input = Some((mode, value));
                                }
                            },
                        },
                        KeyCode::Esc => {}
                        KeyCode::Backspace => {
                            value.pop();
                            input = Some((mode, value));
                            notice = None;
                        }
                        KeyCode::Char(character) => {
                            value.push(character);
                            input = Some((mode, value));
                            notice = None;
                        }
                        _ => input = Some((mode, value)),
                    }
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') => {
                        for worker in workers.values() {
                            let _ = worker.pause.send(true);
                        }
                        for download in &mut shared
                            .lock()
                            .expect("download state mutex poisoned")
                            .downloads
                        {
                            if download.status == Status::Downloading {
                                download.status = Status::Paused;
                            }
                        }
                        for (_, worker) in workers.drain() {
                            let _ = worker.handle.await;
                        }
                        break 'ui;
                    }
                    KeyCode::Char('a') => {
                        notice = None;
                        input = Some((InputMode::Add, String::new()));
                    }
                    KeyCode::Char('s') => {
                        notice = None;
                        input = Some((InputMode::SpeedLimit, String::new()));
                    }
                    KeyCode::Char('?') | KeyCode::F(1) => help_visible = !help_visible,
                    KeyCode::Esc => help_visible = false,
                    KeyCode::Char('+') => adjust_priority(shared, selected, 1),
                    KeyCode::Char('-') => adjust_priority(shared, selected, -1),
                    KeyCode::Char('c') => {
                        let mut state = shared.lock().expect("download state mutex poisoned");
                        let previous_count = state.downloads.len();
                        state
                            .downloads
                            .retain(|download| download.status != Status::Complete);
                        let removed = previous_count - state.downloads.len();
                        selected = selected.min(state.downloads.len().saturating_sub(1));
                        notice = Some(format!("Cleared {removed} completed download(s)"));
                    }
                    KeyCode::Char('j') | KeyCode::Down => {
                        let count = shared
                            .lock()
                            .expect("download state mutex poisoned")
                            .downloads
                            .len();
                        if count > 0 {
                            selected = (selected + 1).min(count - 1);
                        }
                    }
                    KeyCode::Char('k') | KeyCode::Up => {
                        selected = selected.saturating_sub(1);
                    }
                    KeyCode::Char('p') => {
                        if let Some(download) = selected_download(shared, selected) {
                            if download.status == Status::Downloading {
                                if let Some(worker) = workers.get(&download.id) {
                                    let _ = worker.pause.send(true);
                                }
                                set_status(shared, download.id, Status::Paused);
                            }
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(download) = selected_download(shared, selected)
                            && matches!(download.status, Status::Paused | Status::Failed)
                        {
                            set_status(shared, download.id, Status::Queued);
                        }
                    }
                    KeyCode::Char('d') => {
                        let removable = selected_download(shared, selected)
                            .is_some_and(|item| !matches!(item.status, Status::Downloading));
                        if removable {
                            shared
                                .lock()
                                .expect("download state mutex poisoned")
                                .downloads
                                .remove(selected);
                            selected = selected.saturating_sub(1);
                        }
                    }
                    _ => {}
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    drop(event_rx);
    event_reader
        .await
        .context("terminal event reader panicked")?;
    save_state(shared, state_path)?;
    Ok(())
}

fn parse_input(input: &str) -> Result<Option<(String, PathBuf)>> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(None);
    }
    let mut parts = input.splitn(2, char::is_whitespace);
    let url = parts.next().context("missing URL")?.to_owned();
    let output = match parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(path) => PathBuf::from(path),
        None => default_output_path(&url)?,
    };
    reqwest::Url::parse(&url).context("invalid URL")?;
    Ok(Some((url, output)))
}

fn parse_speed_limit(input: &str) -> Result<Option<u64>> {
    let value = input.trim().to_ascii_lowercase();
    if value.is_empty() || matches!(value.as_str(), "off" | "unlimited" | "0") {
        return Ok(None);
    }

    let (number, multiplier) = [
        ("kib/s", 1024_u64),
        ("kb/s", 1024),
        ("kib", 1024),
        ("kb", 1024),
        ("k", 1024),
        ("mib/s", 1024 * 1024),
        ("mb/s", 1024 * 1024),
        ("mib", 1024 * 1024),
        ("mb", 1024 * 1024),
        ("m", 1024 * 1024),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|number| (number.trim(), multiplier))
    })
    .unwrap_or((&value, 1));
    let number = number
        .parse::<u64>()
        .context("speed limit must be a whole number, such as 512K or 2M")?;
    let bytes_per_second = number
        .checked_mul(multiplier)
        .context("speed limit is too large")?;
    if bytes_per_second == 0 {
        bail!("speed limit must be greater than zero, or 'off'");
    }
    Ok(Some(bytes_per_second))
}

fn selected_download(shared: &SharedState, selected: usize) -> Option<Download> {
    shared
        .lock()
        .expect("download state mutex poisoned")
        .downloads
        .get(selected)
        .cloned()
}

fn set_status(shared: &SharedState, id: u64, status: Status) {
    if let Some(download) = shared
        .lock()
        .expect("download state mutex poisoned")
        .downloads
        .iter_mut()
        .find(|download| download.id == id)
    {
        download.status = status;
        download.error = None;
    }
}

fn set_failure(shared: &SharedState, id: u64, error: String) {
    if let Some(download) = shared
        .lock()
        .expect("download state mutex poisoned")
        .downloads
        .iter_mut()
        .find(|download| download.id == id)
    {
        download.status = Status::Failed;
        download.error = Some(error);
    }
}

async fn run_download(
    shared: SharedState,
    client: reqwest::Client,
    download: Download,
    pause: watch::Receiver<bool>,
    rate_limiter: Arc<RateLimiter>,
) -> Result<()> {
    if download.output.exists() {
        bail!("destination already exists: {}", download.output.display());
    }
    if *pause.borrow() {
        set_status(&shared, download.id, Status::Paused);
        return Ok(());
    }

    let probe = client
        .get(&download.url)
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()
        .await
        .with_context(|| format!("request failed: {}", download.url))?;
    let range = probe
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|header| header.to_str().ok())
        .and_then(parse_content_range);
    let supports_ranges = probe.status() == reqwest::StatusCode::PARTIAL_CONTENT
        && range.is_some_and(|(start, end, total)| start == 0 && end == 0 && total > 0);
    let etag = probe
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|header| header.to_str().ok())
        .map(str::to_owned);
    let last_modified = probe
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|header| header.to_str().ok())
        .map(str::to_owned);

    let previous = shared
        .lock()
        .expect("download state mutex poisoned")
        .downloads
        .iter()
        .find(|item| item.id == download.id)
        .cloned()
        .unwrap_or(download.clone());
    let remote_changed = previous
        .etag
        .as_ref()
        .is_some_and(|stored| Some(stored) != etag.as_ref())
        || previous
            .last_modified
            .as_ref()
            .is_some_and(|stored| Some(stored) != last_modified.as_ref());
    if remote_changed {
        for index in 0..MAX_SEGMENTS {
            let path = segment_path(&download.output, index);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("could not discard stale partial file {}", path.display())
                    });
                }
            }
        }
        let legacy_partial = append_suffix(&download.output, ".part");
        match tokio::fs::remove_file(&legacy_partial).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("could not discard stale partial file"),
        }
    }

    if supports_ranges {
        let total = range.context("range probe was missing Content-Range")?.2;
        let count = ((total - 1) / MIN_SEGMENT_BYTES + 1)
            .min(MAX_SEGMENTS as u64)
            .max(1) as usize;
        let segment_size = total.div_ceil(count as u64);
        let segments: Vec<(usize, u64, u64)> = (0..count)
            .filter_map(|index| {
                let start = index as u64 * segment_size;
                (start < total).then_some((index, start, (start + segment_size).min(total) - 1))
            })
            .collect();

        let mut downloaded = 0_u64;
        for (index, start, end) in &segments {
            let path = segment_path(&download.output, *index);
            let length = match tokio::fs::metadata(&path).await {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("could not inspect partial file {}", path.display())
                    });
                }
            };
            if length > end - start + 1 {
                tokio::fs::remove_file(&path).await?;
            } else {
                downloaded += length;
            }
        }

        {
            let mut state = shared.lock().expect("download state mutex poisoned");
            if let Some(item) = state
                .downloads
                .iter_mut()
                .find(|item| item.id == download.id)
            {
                item.status = Status::Downloading;
                item.total = Some(total);
                item.downloaded = downloaded;
                item.etag = etag.clone();
                item.last_modified = last_modified.clone();
                item.error = None;
            }
        }

        let mut join_set = JoinSet::new();
        for (index, start, end) in segments.iter().copied() {
            let path = segment_path(&download.output, index);
            let client = client.clone();
            let url = download.url.clone();
            let shared = Arc::clone(&shared);
            let pause = pause.clone();
            let etag = etag.clone();
            let last_modified = last_modified.clone();
            let task_id = download.id;
            let rate_limiter = Arc::clone(&rate_limiter);
            join_set.spawn(async move {
                download_segment(
                    shared,
                    client,
                    url,
                    task_id,
                    path,
                    start,
                    end,
                    total,
                    etag,
                    last_modified,
                    pause,
                    rate_limiter,
                )
                .await
            });
        }

        let mut paused = false;
        while let Some(result) = join_set.join_next().await {
            match result.context("segment worker stopped unexpectedly")?? {
                true => {}
                false => paused = true,
            }
        }
        if paused {
            set_status(&shared, download.id, Status::Paused);
            return Ok(());
        }
        assemble_segments(&segments, &download.output).await?;
    } else {
        let response = client
            .get(&download.url)
            .send()
            .await
            .with_context(|| format!("request failed: {}", download.url))?
            .error_for_status()
            .context("server returned an error status")?;
        let total = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|header| header.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        {
            let mut state = shared.lock().expect("download state mutex poisoned");
            if let Some(item) = state
                .downloads
                .iter_mut()
                .find(|item| item.id == download.id)
            {
                item.status = Status::Downloading;
                item.total = total;
                item.etag = etag;
                item.last_modified = last_modified;
                item.downloaded = 0;
                item.error = None;
            }
        }
        if !download_single(
            shared.clone(),
            download.clone(),
            response,
            pause,
            rate_limiter,
        )
        .await?
        {
            return Ok(());
        }
    }

    set_status(&shared, download.id, Status::Complete);
    Ok(())
}

async fn download_segment(
    shared: SharedState,
    client: reqwest::Client,
    url: String,
    task_id: u64,
    path: PathBuf,
    start: u64,
    end: u64,
    total: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    mut pause: watch::Receiver<bool>,
    rate_limiter: Arc<RateLimiter>,
) -> Result<bool> {
    let segment_length = end - start + 1;
    let mut existing = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error).context("could not inspect segment file"),
    };
    if existing > segment_length {
        tokio::fs::remove_file(&path).await?;
        existing = 0;
    } else if existing == segment_length {
        return Ok(true);
    }
    if *pause.borrow() {
        return Ok(false);
    }

    let range_start = start + existing;
    let mut request = client
        .get(url)
        .header(reqwest::header::RANGE, format!("bytes={range_start}-{end}"));
    if let Some(value) = etag {
        request = request.header(reqwest::header::IF_RANGE, value);
    } else if let Some(value) = last_modified {
        request = request.header(reqwest::header::IF_RANGE, value);
    }
    let mut response = request.send().await?.error_for_status()?;
    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|header| header.to_str().ok())
        .and_then(parse_content_range);
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
        || !content_range.is_some_and(|(response_start, response_end, response_total)| {
            response_start == range_start && response_end <= end && response_total == total
        })
    {
        bail!("server returned an invalid range response for bytes {range_start}-{end}");
    }
    if *pause.borrow() {
        return Ok(false);
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("could not open {}", path.display()))?;
    loop {
        tokio::select! {
            changed = pause.changed() => {
                if changed.is_err() || *pause.borrow() {
                    file.flush().await?;
                    file.sync_all().await?;
                    return Ok(false);
                }
            }
            chunk = response.chunk() => {
                match chunk? {
                    Some(chunk) => {
                        tokio::select! {
                            changed = pause.changed() => {
                                if changed.is_err() || *pause.borrow() {
                                    file.flush().await?;
                                    file.sync_all().await?;
                                    return Ok(false);
                                }
                            }
                            _ = rate_limiter.acquire(chunk.len()) => {}
                        }
                        file.write_all(&chunk).await?;
                        let mut state = shared.lock().expect("download state mutex poisoned");
                        if let Some(item) = state.downloads.iter_mut().find(|item| item.id == task_id) {
                            item.downloaded += chunk.len() as u64;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    file.sync_all().await?;
    drop(file);
    let actual = tokio::fs::metadata(&path).await?.len();
    if actual != segment_length {
        bail!(
            "segment {} is incomplete: expected {segment_length} bytes, got {actual}",
            path.display()
        );
    }
    Ok(true)
}

async fn download_single(
    shared: SharedState,
    download: Download,
    mut response: reqwest::Response,
    mut pause: watch::Receiver<bool>,
    rate_limiter: Arc<RateLimiter>,
) -> Result<bool> {
    if *pause.borrow() {
        set_status(&shared, download.id, Status::Paused);
        return Ok(false);
    }
    let partial = append_suffix(&download.output, ".part");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&partial)
        .await
        .with_context(|| format!("could not create {}", partial.display()))?;
    loop {
        tokio::select! {
            changed = pause.changed() => {
                if changed.is_err() || *pause.borrow() {
                    file.flush().await?;
                    file.sync_all().await?;
                    set_status(&shared, download.id, Status::Paused);
                    return Ok(false);
                }
            }
            chunk = response.chunk() => {
                match chunk? {
                    Some(chunk) => {
                        tokio::select! {
                            changed = pause.changed() => {
                                if changed.is_err() || *pause.borrow() {
                                    file.flush().await?;
                                    file.sync_all().await?;
                                    set_status(&shared, download.id, Status::Paused);
                                    return Ok(false);
                                }
                            }
                            _ = rate_limiter.acquire(chunk.len()) => {}
                        }
                        file.write_all(&chunk).await?;
                        let mut state = shared.lock().expect("download state mutex poisoned");
                        if let Some(item) = state.downloads.iter_mut().find(|item| item.id == download.id) {
                            item.downloaded += chunk.len() as u64;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&partial, &download.output)
        .await
        .with_context(|| format!("could not finalize {}", download.output.display()))?;
    Ok(true)
}

async fn assemble_segments(segments: &[(usize, u64, u64)], output: &Path) -> Result<()> {
    let assembling = append_suffix(output, ".assembling");
    let mut destination = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&assembling)
        .await
        .with_context(|| format!("could not create {}", assembling.display()))?;
    for (index, _, _) in segments {
        let path = segment_path(output, *index);
        let mut part = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("could not read segment {}", path.display()))?;
        tokio::io::copy(&mut part, &mut destination).await?;
    }
    destination.sync_all().await?;
    drop(destination);
    tokio::fs::rename(&assembling, output)
        .await
        .with_context(|| format!("could not finalize {}", output.display()))?;
    for (index, _, _) in segments {
        tokio::fs::remove_file(segment_path(output, *index))
            .await
            .with_context(|| format!("could not remove completed segment {index}"))?;
    }
    Ok(())
}

fn segment_path(output: &Path, index: usize) -> PathBuf {
    append_suffix(output, &format!(".part.{index}"))
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (unit, value) = value.split_once(' ')?;
    if unit != "bytes" {
        return None;
    }
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?, total.parse().ok()?))
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn draw(
    stdout: &mut io::Stdout,
    shared: &SharedState,
    selected: usize,
    input: Option<&(InputMode, String)>,
    help_visible: bool,
    notice: Option<&str>,
) -> Result<()> {
    let state = shared.lock().expect("download state mutex poisoned");
    let (terminal_width, terminal_height) = size().context("could not read terminal size")?;
    let terminal_width = usize::from(terminal_width);
    let terminal_height = usize::from(terminal_height);
    let footer_height = 4 + usize::from(notice.is_some()) + usize::from(input.is_some());
    let body_height = terminal_height.saturating_sub(4 + footer_height);
    let active = state
        .downloads
        .iter()
        .filter(|download| download.status == Status::Downloading)
        .count();
    let count_status = |status| {
        state
            .downloads
            .iter()
            .filter(|download| download.status == status)
            .count()
    };
    let mut lines = vec![
        frame_top("ZAGE | A LIGHTWEIGHT DOWNLOAD MANAGER", terminal_width),
        frame_content(
            &format!(
                "{active}/{} active | {} queued | {} paused | {} complete | {} failed",
                MAX_ACTIVE_DOWNLOADS,
                count_status(Status::Queued),
                count_status(Status::Paused),
                count_status(Status::Complete),
                count_status(Status::Failed)
            ),
            terminal_width,
        ),
        frame_content(
            &format!(
                "Speed cap: {}",
                state
                    .speed_limit_bps
                    .map(human_rate)
                    .unwrap_or_else(|| "unlimited".to_owned())
            ),
            terminal_width,
        ),
        frame_top(
            if help_visible {
                "HELP / KEYBOARD SHORTCUTS".to_owned()
            } else {
                format!("DOWNLOADS ({})", state.downloads.len())
            }
            .as_str(),
            terminal_width,
        ),
    ];

    let mut body = Vec::new();
    if help_visible {
        body.extend(
            [
                "Navigation: Up/Down arrows or j/k select a download.",
                "Add: a opens a prompt; enter a URL and optional output path.",
                "Pause: p pauses the selected active download.",
                "Resume/retry: r restarts a paused or failed download.",
                "Remove: d removes the selected item when it is not active.",
                "Clear: c removes all completed downloads from the queue.",
                "Priority: + or - changes queued download priority.",
                "Speed: s sets a global cap (examples: 512K, 2M, or off).",
                "Downloads run up to 3 at once, with up to 8 parts per file.",
                "Quit: q saves the queue; unfinished downloads can be resumed.",
                "Press ? or Esc to close this help.",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    } else if state.downloads.is_empty() {
        body.push("Your download queue is empty.".to_owned());
        body.push("Press a to add a URL, or ? to see all controls.".to_owned());
    } else {
        let visible_count = (body_height / 2).max(1);
        let first = selected.saturating_sub(visible_count.saturating_sub(1));
        let last = (first + visible_count).min(state.downloads.len());
        let bar_width = (terminal_width.saturating_sub(50)).clamp(4, 18);

        for (index, download) in state.downloads.iter().enumerate().take(last).skip(first) {
            let marker = if index == selected { ">" } else { " " };
            let filename = download
                .output
                .file_name()
                .unwrap_or_else(|| download.output.as_os_str())
                .to_string_lossy();
            body.push(format!(
                "{marker} {:>4}  {}  [{}]",
                download.id,
                filename,
                download.status.label()
            ));

            if let Some(error) = &download.error {
                body.push(format!("       Error: {error}"));
            } else {
                let (progress, bar) = match download.total {
                    Some(total) if total > 0 => (
                        format!("{:5.1}%", download.downloaded as f64 * 100.0 / total as f64),
                        progress_bar(download.downloaded, Some(total), bar_width),
                    ),
                    _ => (
                        "  ---%".to_owned(),
                        progress_bar(download.downloaded, None, bar_width),
                    ),
                };
                let size = match download.total {
                    Some(total) => format!(
                        "{} / {}",
                        human_bytes(download.downloaded),
                        human_bytes(total)
                    ),
                    None => format!("{} downloaded", human_bytes(download.downloaded)),
                };
                body.push(format!(
                    "       {progress} {bar}  {}  {size}  {} priority",
                    human_rate(download.speed_bps),
                    priority_label(download.priority)
                ));
            }
        }
    }

    body.truncate(body_height);
    for line in &body {
        lines.push(frame_content(line, terminal_width));
    }
    for _ in body.len()..body_height {
        lines.push(frame_content("", terminal_width));
    }

    lines.push(frame_middle(terminal_width));
    lines.push(frame_content(
        "a Add  p Pause  r Resume/retry  d Remove  c Clear completed",
        terminal_width,
    ));
    lines.push(frame_content(
        "↑/↓ or j/k Select  +/- Priority  s Speed cap  ? Help  q Quit",
        terminal_width,
    ));
    if let Some(notice) = notice {
        lines.push(frame_content(&format!("! {notice}"), terminal_width));
    }
    if let Some((mode, value)) = input {
        let prompt = match mode {
            InputMode::Add => "URL [optional output path]",
            InputMode::SpeedLimit => "Speed limit [e.g. 2M, or off]",
        };
        lines.push(frame_content(
            &format!("{prompt}: {value}_"),
            terminal_width,
        ));
    }
    lines.push(frame_bottom(terminal_width));

    lines.truncate(terminal_height);
    execute!(stdout, MoveTo(0, 0), Clear(ClearType::All))?;
    for (row, line) in lines.iter().enumerate() {
        execute!(stdout, MoveTo(0, row as u16))?;
        write!(stdout, "{line}")?;
    }
    stdout.flush()?;
    Ok(())
}

fn frame_top(label: &str, width: usize) -> String {
    if width < 2 {
        return "─".repeat(width);
    }
    let interior = width - 2;
    let text = clip_text(&format!("─ {label} "), interior);
    format!("┌{text}{}┐", "─".repeat(interior - text.chars().count()))
}

fn frame_middle(width: usize) -> String {
    if width < 2 {
        return "─".repeat(width);
    }
    format!("├{}┤", "─".repeat(width - 2))
}

fn frame_bottom(width: usize) -> String {
    if width < 2 {
        return "─".repeat(width);
    }
    format!("└{}┘", "─".repeat(width - 2))
}

fn frame_content(content: &str, width: usize) -> String {
    if width < 2 {
        return clip_text(content, width);
    }
    let interior = width - 2;
    let content = clip_text(content, interior);
    format!("│{content:<interior$}│")
}

fn clip_text(text: &str, width: usize) -> String {
    text.chars().take(width).collect()
}

fn progress_bar(downloaded: u64, total: Option<u64>, width: usize) -> String {
    let filled = total
        .filter(|total| *total > 0)
        .map(|total| ((downloaded.min(total) as u128 * width as u128) / total as u128) as usize)
        .unwrap_or(0);
    format!(
        "[{}{}]",
        "█".repeat(filled),
        "░".repeat(width.saturating_sub(filled))
    )
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    format!("{value:.1} {}", UNITS[unit])
}

fn human_rate(bytes_per_second: u64) -> String {
    format!("{}/s", human_bytes(bytes_per_second))
}

fn priority_label(priority: u8) -> &'static str {
    match priority {
        0 => "low",
        2.. => "high",
        _ => "normal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_range_components() {
        assert_eq!(parse_content_range("bytes 12-29/100"), Some((12, 29, 100)));
        assert_eq!(parse_content_range("items 12-29/100"), None);
        assert_eq!(parse_content_range("bytes 12-29/*"), None);
    }

    #[test]
    fn derives_filename_from_url_path_not_query() {
        assert_eq!(
            default_output_path("https://example.test/files/archive.tar?token=abc").unwrap(),
            PathBuf::from("archive.tar")
        );
        assert_eq!(
            default_output_path("https://example.test/").unwrap(),
            PathBuf::from("download")
        );
    }

    #[test]
    fn parses_optional_output_path() {
        assert_eq!(
            parse_input("https://example.test/file.bin ./downloads/file.bin").unwrap(),
            Some((
                "https://example.test/file.bin".to_owned(),
                PathBuf::from("./downloads/file.bin")
            ))
        );
        assert!(parse_input("not a url").is_err());
    }

    #[test]
    fn parses_global_speed_limits() {
        assert_eq!(parse_speed_limit("512K").unwrap(), Some(512 * 1024));
        assert_eq!(parse_speed_limit("2MiB/s").unwrap(), Some(2 * 1024 * 1024));
        assert_eq!(parse_speed_limit("off").unwrap(), None);
        assert!(parse_speed_limit("fast").is_err());
    }

    #[tokio::test]
    async fn cancelling_rate_wait_releases_the_shared_limiter() {
        let limiter = Arc::new(RateLimiter::new(Some(1)));
        let (cancel, mut receiver) = watch::channel(false);
        let waiting = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move {
                tokio::select! {
                    _ = limiter.acquire(1024 * 1024) => false,
                    _ = receiver.changed() => true,
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.send(true).unwrap();
        assert!(waiting.await.unwrap());

        limiter.set_limit(None);
        tokio::time::timeout(Duration::from_millis(100), limiter.acquire(1))
            .await
            .expect("cancelled rate wait retained the limiter");
    }
}
