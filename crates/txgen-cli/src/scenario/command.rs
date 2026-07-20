use super::{
    error::StepError,
    schema::{is_path_like_program, CommandStdout, CommandStep},
    value::{eval_expression, RuntimeContext, RuntimeValue},
};
use eyre::{bail, Result};
use std::{fs, io, process::Stdio};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::Instant,
};

const MAX_COMMAND_STREAM_BYTES: usize = 1024 * 1024;
#[cfg(unix)]
const MINIMAL_COMMAND_PATH: &str = "/usr/local/bin:/usr/bin:/bin";
#[cfg(windows)]
const MINIMAL_COMMAND_PATH: &str = r"C:\Windows\System32;C:\Windows";
#[cfg(not(any(unix, windows)))]
const MINIMAL_COMMAND_PATH: &str = "";

/// Validate static command paths before scenario initialization performs any work.
pub(super) fn preflight(step: &CommandStep) -> Result<()> {
    if step.program.as_os_str().is_empty() {
        bail!("command program must not be empty");
    }

    // A bare program name is intentionally resolved through PATH by the child
    // launcher. Paths containing a directory component can be checked eagerly.
    if is_path_like_program(&step.program) {
        let metadata = match fs::metadata(&step.program) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                bail!("command program does not exist: {}", step.program.display())
            }
            Err(_) => {
                bail!("command program cannot be inspected: {}", step.program.display())
            }
        };
        if !metadata.is_file() {
            bail!("command program is not a file: {}", step.program.display());
        }
        #[cfg(unix)]
        if !is_executable(&metadata) {
            bail!("command program is not executable: {}", step.program.display());
        }
    }

    if let Some(cwd) = &step.cwd {
        let metadata = match fs::metadata(cwd) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                bail!("command cwd does not exist: {}", cwd.display())
            }
            Err(_) => bail!("command cwd cannot be inspected: {}", cwd.display()),
        };
        if !metadata.is_dir() {
            bail!("command cwd is not a directory: {}", cwd.display());
        }
    }

    Ok(())
}

/// Execute one command step, returning its parsed stdout without exposing
/// command inputs or output through an error diagnostic.
pub(super) async fn execute_command(
    step: &CommandStep,
    context: &RuntimeContext,
    deadline: Instant,
) -> Result<RuntimeValue, StepError> {
    if deadline <= Instant::now() {
        return Err(StepError::timeout());
    }

    let args = step
        .args
        .iter()
        .map(|value| {
            eval_expression(value, context)
                .and_then(|value| value.to_process_arg())
                .map_err(|_| StepError::command_input_invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let environment = step
        .env
        .iter()
        .map(|(name, value)| {
            eval_expression(value, context)
                .and_then(|value| value.to_process_arg())
                .map(|value| (name, value))
                .map_err(|_| StepError::command_input_invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;

    if deadline <= Instant::now() {
        return Err(StepError::timeout());
    }

    let mut command = Command::new(&step.program);
    command
        .args(args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.env("PATH", MINIMAL_COMMAND_PATH);
    for (name, value) in environment {
        command.env(name, value);
    }
    if let Some(cwd) = &step.cwd {
        command.current_dir(cwd);
    }
    configure_process_group(&mut command);

    let mut child = command.spawn().map_err(|_| StepError::command_spawn())?;
    let mut process_group = ProcessGroupGuard::new(child.id());
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        terminate_and_reap(&mut child, &mut process_group).await;
        return Err(StepError::command_io());
    };

    let (signal_sender, mut signal_receiver) = mpsc::channel(4);
    let mut stdout_task = tokio::spawn(read_bounded(stdout, signal_sender.clone()));
    let mut stderr_task = tokio::spawn(read_bounded(stderr, signal_sender));

    let status = {
        let wait = child.wait();
        tokio::pin!(wait);
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => Err(StepError::timeout()),
            Some(signal) = signal_receiver.recv() => Err(signal.error()),
            status = &mut wait => status.map_err(|_| StepError::command_io()),
        }
    };
    let status = match status {
        Ok(status) => status,
        Err(error) => {
            terminate_and_reap(&mut child, &mut process_group).await;
            abort_readers(&mut stdout_task, &mut stderr_task).await;
            return Err(error);
        }
    };

    let output = {
        let readers = async { tokio::join!(&mut stdout_task, &mut stderr_task) };
        tokio::pin!(readers);
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => Err(StepError::timeout()),
            Some(signal) = signal_receiver.recv() => Err(signal.error()),
            output = &mut readers => decode_reader_results(output),
        }
    };
    let (stdout, stderr) = match output {
        Ok(output) => output,
        Err(error) => {
            // The direct child may already have exited while a descendant still
            // owns one of its pipes, so terminate the process group here too.
            terminate_and_reap(&mut child, &mut process_group).await;
            abort_readers(&mut stdout_task, &mut stderr_task).await;
            return Err(error);
        }
    };

    // A descendant may close the captured pipes and remain alive after the
    // direct child exits. It is still part of this command's dedicated process
    // group and must not outlive the scenario step or its account leases.
    process_group.kill();

    if stdout.exceeded || stderr.exceeded {
        return Err(StepError::command_output_too_large());
    }
    if !status.success() {
        return Err(StepError::command_exit_nonzero());
    }

    match step.stdout {
        CommandStdout::Json => {
            let json = serde_json::from_slice(&stdout.bytes)
                .map_err(|_| StepError::command_output_invalid())?;
            RuntimeValue::from_json(&json).map_err(|_| StepError::command_output_invalid())
        }
    }
}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o111 != 0
}

#[derive(Debug, Clone, Copy)]
enum ReaderSignal {
    OutputTooLarge,
    IoError,
}

impl ReaderSignal {
    fn error(self) -> StepError {
        match self {
            Self::OutputTooLarge => StepError::command_output_too_large(),
            Self::IoError => StepError::command_io(),
        }
    }
}

struct CapturedStream {
    bytes: Vec<u8>,
    exceeded: bool,
}

async fn read_bounded<R>(
    mut reader: R,
    signal_sender: mpsc::Sender<ReaderSignal>,
) -> io::Result<CapturedStream>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut buffer = [0_u8; 8192];

    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(read) => read,
            Err(error) => {
                let _ = signal_sender.try_send(ReaderSignal::IoError);
                return Err(error);
            }
        };
        if read == 0 {
            break;
        }

        let remaining = MAX_COMMAND_STREAM_BYTES.saturating_sub(bytes.len());
        bytes.extend_from_slice(&buffer[..read.min(remaining)]);
        if read > remaining && !exceeded {
            exceeded = true;
            let _ = signal_sender.try_send(ReaderSignal::OutputTooLarge);
        }
    }

    Ok(CapturedStream { bytes, exceeded })
}

fn decode_reader_results(
    output: (
        Result<io::Result<CapturedStream>, tokio::task::JoinError>,
        Result<io::Result<CapturedStream>, tokio::task::JoinError>,
    ),
) -> Result<(CapturedStream, CapturedStream), StepError> {
    let stdout =
        output.0.map_err(|_| StepError::command_io())?.map_err(|_| StepError::command_io())?;
    let stderr =
        output.1.map_err(|_| StepError::command_io())?.map_err(|_| StepError::command_io())?;
    Ok((stdout, stderr))
}

async fn abort_readers(
    stdout: &mut JoinHandle<io::Result<CapturedStream>>,
    stderr: &mut JoinHandle<io::Result<CapturedStream>>,
) {
    stdout.abort();
    stderr.abort();
    let _ = stdout.await;
    let _ = stderr.await;
}

async fn terminate_and_reap(child: &mut Child, process_group: &mut ProcessGroupGuard) {
    process_group.kill();
    let _ = child.start_kill();
    let _ = child.wait().await;
}

struct ProcessGroupGuard {
    id: Option<u32>,
    armed: bool,
}

impl ProcessGroupGuard {
    fn new(id: Option<u32>) -> Self {
        Self { id, armed: true }
    }

    fn kill(&mut self) {
        if self.armed {
            kill_process_group(self.id);
            self.armed = false;
        }
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.as_std_mut().process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn kill_process_group(process_group_id: Option<u32>) {
    let Some(pid) = process_group_id.and_then(|pid| i32::try_from(pid).ok()) else { return };

    const SIGKILL: i32 = 9;
    unsafe extern "C" {
        #[link_name = "kill"]
        fn libc_kill(pid: i32, signal: i32) -> i32;
    }

    // SAFETY: `pid` came from the spawned child and negating it selects the
    // dedicated process group configured immediately before spawn. SIGKILL has
    // the same numeric value on supported Unix targets.
    unsafe {
        let _ = libc_kill(-pid, SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_process_group_id: Option<u32>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, time::Duration};

    #[cfg(unix)]
    fn shell_step(script: impl Into<String>) -> CommandStep {
        CommandStep {
            program: "/bin/sh".into(),
            args: vec![
                serde_yaml::Value::String("-c".to_string()),
                serde_yaml::Value::String(script.into()),
            ],
            env: BTreeMap::new(),
            cwd: None,
            stdout: CommandStdout::Json,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn parses_json_and_clears_unspecified_environment() {
        let step = shell_step(format!(
            r#"if [ -n "${{HOME+x}}" ] || [ "$PATH" != '{}' ]; then exit 9; fi; printf '{{"ok":true}}'"#,
            MINIMAL_COMMAND_PATH
        ));
        let result = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

        let RuntimeValue::Object(result) = result else { panic!("expected object") };
        assert_eq!(result["ok"], RuntimeValue::Bool(true));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn materializes_arguments_and_explicit_environment() {
        let variable: serde_yaml::Value = serde_yaml::from_str("{ var: input }").unwrap();
        let mut step = shell_step(r#"printf '{"arg":"%s","env":"%s"}' "$1" "$VALUE""#);
        step.args.push(serde_yaml::Value::String("txgen-test".to_string()));
        step.args.push(variable.clone());
        step.env.insert("VALUE".to_string(), variable);
        let context = RuntimeContext::new(BTreeMap::from([(
            "input".to_string(),
            RuntimeValue::String("resolved".to_string()),
        )]))
        .unwrap();

        let result = execute_command(&step, &context, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();

        let RuntimeValue::Object(result) = result else { panic!("expected object") };
        assert_eq!(result["arg"], RuntimeValue::String("resolved".to_string()));
        assert_eq!(result["env"], RuntimeValue::String("resolved".to_string()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_failures_do_not_expose_process_data() {
        const MARKER: &str = "txgen-sensitive-marker";
        let step = shell_step(format!("printf '{MARKER}'; printf '{MARKER}' >&2; exit 7"));
        let error = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.classification, "command_exit_nonzero");
        assert!(!error.to_string().contains(MARKER));
        assert!(!error.sanitized_detail().unwrap().contains(MARKER));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_more_than_one_json_value() {
        let step = shell_step("printf '{} {}'");
        let error = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.classification, "command_output_invalid");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_failure_uses_a_fixed_diagnostic() {
        const MARKER: &str = "txgen-sensitive-program-marker";
        let step = CommandStep {
            program: MARKER.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
            stdout: CommandStdout::Json,
        };
        let error = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.classification, "command_spawn_error");
        assert!(!error.to_string().contains(MARKER));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_oversized_output() {
        let step = shell_step(format!("head -c {} /dev/zero", MAX_COMMAND_STREAM_BYTES + 1));
        let error = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.classification, "command_output_too_large");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_background_descendants() {
        let directory = std::env::temp_dir().join(format!(
            "txgen-command-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let sentinel = directory.join("sentinel");
        let step = shell_step(format!("(sleep 1; touch '{}') & exit 0", sentinel.display()));

        let error = execute_command(
            &step,
            &RuntimeContext::empty(),
            Instant::now() + Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert_eq!(error.classification, "timeout");
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(!sentinel.exists());

        fs::remove_dir(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_kills_background_descendants() {
        let directory = std::env::temp_dir().join(format!(
            "txgen-command-cancel-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let started = directory.join("started");
        let sentinel = directory.join("sentinel");
        let step = shell_step(format!(
            "touch '{}'; (sleep 1; touch '{}') & exit 0",
            started.display(),
            sentinel.display()
        ));
        let task = tokio::spawn(async move {
            execute_command(
                &step,
                &RuntimeContext::empty(),
                Instant::now() + Duration::from_secs(30),
            )
            .await
        });

        let wait_started = async {
            while !started.exists() {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(Duration::from_secs(2), wait_started).await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(!sentinel.exists());

        fs::remove_file(started).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn preflight_checks_explicit_paths_but_defers_bare_program_names() {
        use std::os::unix::fs::PermissionsExt;

        let mut step = CommandStep {
            program: "resolved-through-path".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
            stdout: CommandStdout::Json,
        };
        preflight(&step).unwrap();

        step.program = "/definitely/not/a/txgen-command".into();
        assert!(preflight(&step).unwrap_err().to_string().contains("does not exist"));

        let directory = std::env::temp_dir().join(format!(
            "txgen-command-preflight-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let program = directory.join("program");
        fs::write(&program, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o600)).unwrap();
        step.program = program.clone();
        assert!(preflight(&step).unwrap_err().to_string().contains("not executable"));
        fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
        preflight(&step).unwrap();

        fs::remove_file(program).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
