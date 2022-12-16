mod common;
use anyhow::Context;
use common::docker::{pull_images, DockerTestClient, TestContainerService};
use futures::{StreamExt, TryStreamExt};
use graph_tests::helpers::{
    basename, get_unique_ganache_counter, get_unique_postgres_counter, make_ganache_uri,
    make_ipfs_uri, make_postgres_uri, pretty_output, GraphNodePorts, MappedPorts,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

/// All directories containing integration tests to run in parallel.
pub const PARALLEL_TEST_DIRECTORIES: &[&str] = &[
    "api-version-v0-0-4",
    "ganache-reverts",
    "host-exports",
    "non-fatal-errors",
    "overloaded-contract-functions",
    "poi-for-failed-subgraph",
    "remove-then-update",
    "value-roundtrip",
];

#[derive(Debug, Clone)]
struct TestSettings {
    n_parallel_tests: u64,
    ganache_hard_wait: Duration,
    ipfs_hard_wait: Duration,
    postgres_hard_wait: Duration,
}

impl TestSettings {
    /// Automatically fills in missing env. vars. with defaults.
    ///
    /// # Panics
    ///
    /// Panics if any of the env. vars. is set incorrectly.
    pub fn from_env() -> Self {
        Self {
            n_parallel_tests: parse_numeric_environment_variable("N_CONCURRENT_TESTS")
                .unwrap_or(15),
            ganache_hard_wait: Duration::from_secs(
                parse_numeric_environment_variable("TESTS_GANACHE_HARD_WAIT_SECONDS").unwrap_or(10),
            ),
            ipfs_hard_wait: Duration::from_secs(
                parse_numeric_environment_variable("TESTS_IPFS_HARD_WAIT_SECONDS").unwrap_or(0),
            ),
            postgres_hard_wait: Duration::from_secs(
                parse_numeric_environment_variable("TESTS_POSTGRES_HARD_WAIT_SECONDS").unwrap_or(0),
            ),
        }
    }
}

/// Contains all information a test command needs
#[derive(Debug)]
struct IntegrationTestSetup {
    postgres_uri: String,
    ipfs_uri: String,
    ganache_port: u16,
    ganache_uri: String,
    graph_node_ports: GraphNodePorts,
    graph_node_bin: Arc<PathBuf>,
    test_directory: PathBuf,
}

impl IntegrationTestSetup {
    fn test_name(&self) -> String {
        basename(&self.test_directory)
    }

    fn graph_node_admin_uri(&self) -> String {
        let ws_port = self.graph_node_ports.admin;
        format!("http://localhost:{}/", ws_port)
    }
}

/// Info about a finished test command
#[derive(Debug)]
struct TestCommandResults {
    success: bool,
    _exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

#[derive(Debug)]
struct StdIO {
    stdout: Option<String>,
    stderr: Option<String>,
}

impl std::fmt::Display for StdIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stdout) = self.stdout {
            write!(f, "{}", stdout)?;
        }
        if let Some(ref stderr) = self.stderr {
            write!(f, "{}", stderr)?
        }
        Ok(())
    }
}

// The results of a finished integration test
#[derive(Debug)]
struct IntegrationTestResult {
    test_setup: IntegrationTestSetup,
    test_command_results: TestCommandResults,
    graph_node_stdio: StdIO,
}

impl IntegrationTestResult {
    fn print_outcome(&self) {
        let status = match self.test_command_results.success {
            true => "SUCCESS",
            false => "FAILURE",
        };
        println!("- Test: {}: {}", status, self.test_setup.test_name())
    }

    fn print_failure(&self) {
        if self.test_command_results.success {
            return;
        }
        let test_name = self.test_setup.test_name();
        println!("=============");
        println!("\nFailed test: {}", test_name);
        println!("-------------");
        println!("{:#?}", self.test_setup);
        println!("-------------");
        println!("\nFailed test command output:");
        println!("---------------------------");
        println!("{}", self.test_command_results.stdout);
        println!("{}", self.test_command_results.stderr);
        println!("--------------------------");
        println!("graph-node command output:");
        println!("--------------------------");
        println!("{}", self.graph_node_stdio);
    }
}

async fn start_service_container(
    service: TestContainerService,
    wait_msg: &str,
    hard_wait: Duration,
) -> anyhow::Result<(Arc<DockerTestClient>, Arc<MappedPorts>)> {
    let docker_client = DockerTestClient::start(service).await.context(format!(
        "Failed to start container service `{}`",
        service.name()
    ))?;

    docker_client
        .wait_for_message(wait_msg.as_bytes(), hard_wait)
        .await
        .context(format!(
            "failed to wait for {} container to be ready to accept connections",
            service.name()
        ))?;

    let ports = docker_client.exposed_ports().await.context(format!(
        "failed to obtain exposed ports for the `{}` container",
        service.name()
    ))?;

    Ok((Arc::new(docker_client), Arc::new(ports)))
}

/// The main test entrypoint
#[tokio::test]
async fn parallel_integration_tests() -> anyhow::Result<()> {
    let test_settings = TestSettings::from_env();

    let current_working_directory =
        std::env::current_dir().context("failed to identify working directory")?;
    let integration_tests_root_directory = current_working_directory.join("parallel-tests");
    let test_directories = PARALLEL_TEST_DIRECTORIES
        .iter()
        .map(|p| integration_tests_root_directory.join(PathBuf::from(p)))
        .collect::<Vec<PathBuf>>();

    // Show discovered tests
    println!("Found {} integration test(s):", test_directories.len());
    for dir in &test_directories {
        println!("  - {}", basename(dir));
    }

    tokio::join!(
        // Pull the required Docker images.
        pull_images(),
        // Run `yarn` command to build workspace.
        run_yarn_command(&integration_tests_root_directory),
    );

    // start docker containers for Postgres and IPFS and wait for them to be ready
    let (postgres, ipfs) = tokio::try_join!(
        start_service_container(
            TestContainerService::Postgres,
            "database system is ready to accept connections",
            test_settings.postgres_hard_wait
        ),
        start_service_container(
            TestContainerService::Ipfs,
            "Daemon is ready",
            test_settings.ipfs_hard_wait
        ),
    )?;

    let graph_node = Arc::new(
        fs::canonicalize("../target/debug/graph-node")
            .context("failed to infer `graph-node` program location. (Was it built already?)")?,
    );

    let stream = tokio_stream::iter(test_directories)
        .map(|dir| {
            run_integration_test(
                dir,
                postgres.0.clone(),
                postgres.1.clone(),
                ipfs.1.clone(),
                graph_node.clone(),
                test_settings.ganache_hard_wait,
            )
        })
        .buffered(test_settings.n_parallel_tests as usize);

    let test_results: Vec<IntegrationTestResult> = stream.try_collect().await?;
    let failed = test_results.iter().any(|r| !r.test_command_results.success);

    // Stop service containers.
    tokio::try_join!(
        async {
            postgres
                .0
                .stop()
                .await
                .context("failed to stop container service for Postgres")
        },
        async {
            ipfs.0
                .stop()
                .await
                .context("failed to stop container service for IPFS")
        },
    )?;

    // print failures
    for failed_test in test_results
        .iter()
        .filter(|t| !t.test_command_results.success)
    {
        failed_test.print_failure()
    }

    // print test result summary
    println!("\nTest results:");
    for test_result in &test_results {
        test_result.print_outcome()
    }

    if failed {
        Err(anyhow::anyhow!("Some tests have failed"))
    } else {
        Ok(())
    }
}

/// Prepare and run the integration test
async fn run_integration_test(
    test_directory: PathBuf,
    postgres_docker: Arc<DockerTestClient>,
    postgres_ports: Arc<MappedPorts>,
    ipfs_ports: Arc<MappedPorts>,
    graph_node_bin: Arc<PathBuf>,
    ganache_hard_wait: Duration,
) -> anyhow::Result<IntegrationTestResult> {
    // start a dedicated ganache container for this test
    let unique_ganache_counter = get_unique_ganache_counter();
    let ganache = DockerTestClient::start(TestContainerService::Ganache(unique_ganache_counter))
        .await
        .context("failed to start container service for Ganache.")?;
    ganache
        .wait_for_message(b"Listening on ", ganache_hard_wait)
        .await
        .context("failed to wait for Ganache container to be ready to accept connections")?;

    let ganache_ports: MappedPorts = ganache
        .exposed_ports()
        .await
        .context("failed to obtain exposed ports for Ganache container")?;

    // build URIs
    let postgres_unique_id = get_unique_postgres_counter();

    let postgres_uri = make_postgres_uri(&postgres_unique_id, &postgres_ports);
    let ipfs_uri = make_ipfs_uri(&ipfs_ports);
    let (ganache_port, ganache_uri) = make_ganache_uri(&ganache_ports);

    // create test database
    DockerTestClient::create_postgres_database(&postgres_docker, &postgres_unique_id)
        .await
        .context("failed to create the test database.")?;

    // prepare to run test comand
    let test_setup = IntegrationTestSetup {
        postgres_uri,
        ipfs_uri,
        ganache_uri,
        ganache_port,
        graph_node_bin,
        graph_node_ports: GraphNodePorts::get_ports(),
        test_directory,
    };

    // spawn graph-node
    let mut graph_node_child_command = run_graph_node(&test_setup).await?;

    println!("Test started: {}", basename(&test_setup.test_directory));
    let test_command_results = run_test_command(&test_setup).await?;

    // stop graph-node

    let graph_node_stdio = stop_graph_node(&mut graph_node_child_command).await?;
    // stop ganache container
    ganache
        .stop()
        .await
        .context("failed to stop container service for Ganache")?;

    Ok(IntegrationTestResult {
        test_setup,
        test_command_results,
        graph_node_stdio,
    })
}

/// Runs a command for a integration test
async fn run_test_command(test_setup: &IntegrationTestSetup) -> anyhow::Result<TestCommandResults> {
    let output = Command::new("yarn")
        .arg("test")
        .env("GANACHE_TEST_PORT", test_setup.ganache_port.to_string())
        .env("GRAPH_NODE_ADMIN_URI", test_setup.graph_node_admin_uri())
        .env(
            "GRAPH_NODE_HTTP_PORT",
            test_setup.graph_node_ports.http.to_string(),
        )
        .env(
            "GRAPH_NODE_INDEX_PORT",
            test_setup.graph_node_ports.index.to_string(),
        )
        .env("IPFS_URI", &test_setup.ipfs_uri)
        .current_dir(&test_setup.test_directory)
        .output()
        .await
        .context("failed to run test command")?;

    let test_name = test_setup.test_name();
    let stdout_tag = format!("[{}:stdout] ", test_name);
    let stderr_tag = format!("[{}:stderr] ", test_name);

    Ok(TestCommandResults {
        success: output.status.success(),
        _exit_code: output.status.code(),
        stdout: pretty_output(&output.stdout, &stdout_tag),
        stderr: pretty_output(&output.stderr, &stderr_tag),
    })
}
async fn run_graph_node(test_setup: &IntegrationTestSetup) -> anyhow::Result<Child> {
    use std::process::Stdio;

    let mut command = Command::new(test_setup.graph_node_bin.as_os_str());
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // postgres
        .arg("--postgres-url")
        .arg(&test_setup.postgres_uri)
        // ethereum
        .arg("--ethereum-rpc")
        .arg(&test_setup.ganache_uri)
        // ipfs
        .arg("--ipfs")
        .arg(&test_setup.ipfs_uri)
        // http port
        .arg("--http-port")
        .arg(test_setup.graph_node_ports.http.to_string())
        // index node port
        .arg("--index-node-port")
        .arg(test_setup.graph_node_ports.index.to_string())
        // ws  port
        .arg("--ws-port")
        .arg(test_setup.graph_node_ports.ws.to_string())
        // admin  port
        .arg("--admin-port")
        .arg(test_setup.graph_node_ports.admin.to_string())
        // metrics  port
        .arg("--metrics-port")
        .arg(test_setup.graph_node_ports.metrics.to_string());

    command
        .spawn()
        .context("failed to start graph-node command.")
}

async fn stop_graph_node(child: &mut Child) -> anyhow::Result<StdIO> {
    child.kill().await.context("Failed to kill graph-node")?;

    // capture stdio
    let stdout = match child.stdout.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stdout] ").await?),
        None => None,
    };
    let stderr = match child.stderr.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stderr] ").await?),
        None => None,
    };

    Ok(StdIO { stdout, stderr })
}

async fn process_stdio<T: AsyncReadExt + Unpin>(
    stdio: &mut T,
    prefix: &str,
) -> anyhow::Result<String> {
    let mut buffer: Vec<u8> = Vec::new();
    stdio
        .read_to_end(&mut buffer)
        .await
        .context("failed to read stdio")?;
    Ok(pretty_output(&buffer, prefix))
}

/// run yarn to build everything
async fn run_yarn_command(base_directory: &impl AsRef<Path>) {
    let timer = std::time::Instant::now();
    println!("Running `yarn` command in integration tests root directory.");
    let output = Command::new("yarn")
        .current_dir(base_directory)
        .output()
        .await
        .expect("failed to run yarn command");

    if output.status.success() {
        println!("`yarn` command finished in {}s", timer.elapsed().as_secs());
        return;
    }
    println!("Yarn command failed.");
    println!("{}", pretty_output(&output.stdout, "[yarn:stdout]"));
    println!("{}", pretty_output(&output.stderr, "[yarn:stderr]"));
    panic!("Yarn command failed.")
}

fn parse_numeric_environment_variable(environment_variable_name: &str) -> Option<u64> {
    std::env::var(environment_variable_name)
        .ok()
        .and_then(|x| x.parse().ok())
}
