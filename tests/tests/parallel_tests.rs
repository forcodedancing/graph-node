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

/// All directories containing integration tests to run.
///
/// Hardcoding these paths seems "wrong", and we very well could obtain this
/// list with some directory listing magic. That would, however, also
/// require us to filter out `node_modules`, support files, etc.. Hardly worth
/// it.
pub const INTEGRATION_TEST_DIRS: &[&str] = &[
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
struct IntegrationTestSettings {
    n_parallel_tests: u64,
    ganache_hard_wait: Duration,
    ipfs_hard_wait: Duration,
    postgres_hard_wait: Duration,
}

impl IntegrationTestSettings {
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

/// An aggregator of all configuration and settings required to run a single
/// integration test.
#[derive(Debug)]
struct IntegrationTestRecipe {
    postgres_uri: String,
    ipfs_uri: String,
    ganache_port: u16,
    ganache_uri: String,
    graph_node_ports: GraphNodePorts,
    graph_node_bin: Arc<PathBuf>,
    test_directory: PathBuf,
}

impl IntegrationTestRecipe {
    fn test_name(&self) -> String {
        basename(&self.test_directory)
    }

    fn graph_node_admin_uri(&self) -> String {
        format!("http://localhost:{}/", self.graph_node_ports.admin)
    }
}

/// Info about a finished test command
#[derive(Debug)]
struct IntegrationTestResult {
    success: bool,
    _exit_status_code: Option<i32>,
    output: Output,
}

#[derive(Debug)]
struct Output {
    stdout: Option<String>,
    stderr: Option<String>,
}

impl std::fmt::Display for Output {
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
struct IntegrationTestSummary {
    test_recipe: IntegrationTestRecipe,
    test_command_result: IntegrationTestResult,
    graph_node_output: Output,
}

impl IntegrationTestSummary {
    fn print_outcome(&self) {
        let status = match self.test_command_result.success {
            true => "SUCCESS",
            false => "FAILURE",
        };
        println!("- Test: {}: {}", status, self.test_recipe.test_name())
    }

    fn print_failure(&self) {
        if self.test_command_result.success {
            return;
        }
        let test_name = self.test_recipe.test_name();
        println!("=============");
        println!("\nFailed test: {}", test_name);
        println!("-------------");
        println!("{:#?}", self.test_recipe);
        println!("-------------");
        println!("\nFailed test command output:");
        println!("---------------------------");
        println!("{}", self.test_command_result.output);
        println!("--------------------------");
        println!("graph-node command output:");
        println!("--------------------------");
        println!("{}", self.graph_node_output);
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
    let test_settings = IntegrationTestSettings::from_env();

    let current_working_dir =
        std::env::current_dir().context("failed to identify working directory")?;
    let yarn_workspace_dir = current_working_dir.join("parallel-tests");
    let test_dirs = INTEGRATION_TEST_DIRS
        .iter()
        .map(|p| yarn_workspace_dir.join(PathBuf::from(p)))
        .collect::<Vec<PathBuf>>();

    // Show discovered tests
    println!("Found {} integration test(s):", test_dirs.len());
    for dir in &test_dirs {
        println!("  - {}", basename(dir));
    }

    tokio::join!(
        // Pull the required Docker images.
        pull_images(),
        // Run `yarn` command to build workspace.
        run_yarn_command(&yarn_workspace_dir),
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

    let stream = tokio_stream::iter(test_dirs)
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

    let test_results: Vec<IntegrationTestSummary> = stream.try_collect().await?;
    let failed = test_results.iter().any(|r| !r.test_command_result.success);

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
        .filter(|t| !t.test_command_result.success)
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
) -> anyhow::Result<IntegrationTestSummary> {
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

    // prepare to run test command
    let test_recipe = IntegrationTestRecipe {
        postgres_uri,
        ipfs_uri,
        ganache_uri,
        ganache_port,
        graph_node_bin,
        graph_node_ports: GraphNodePorts::get_ports(),
        test_directory,
    };

    // Spawn graph-node.
    let mut graph_node_child_command = run_graph_node(&test_recipe).await?;

    println!("Test started: {}", basename(&test_recipe.test_directory));
    let result = run_test_command(&test_recipe).await?;

    // Stop graph-node and read its output.
    let graph_node_output = stop_graph_node(&mut graph_node_child_command).await?;

    // Stop Ganache.
    ganache
        .stop()
        .await
        .context("failed to stop container service for Ganache")?;

    Ok(IntegrationTestSummary {
        test_recipe,
        test_command_result: result,
        graph_node_output,
    })
}

/// Runs a command for a integration test
async fn run_test_command(
    test_recipe: &IntegrationTestRecipe,
) -> anyhow::Result<IntegrationTestResult> {
    let output = Command::new("yarn")
        .arg("test")
        .env("GANACHE_TEST_PORT", test_recipe.ganache_port.to_string())
        .env("GRAPH_NODE_ADMIN_URI", test_recipe.graph_node_admin_uri())
        .env(
            "GRAPH_NODE_HTTP_PORT",
            test_recipe.graph_node_ports.http.to_string(),
        )
        .env(
            "GRAPH_NODE_INDEX_PORT",
            test_recipe.graph_node_ports.index.to_string(),
        )
        .env("IPFS_URI", &test_recipe.ipfs_uri)
        .current_dir(&test_recipe.test_directory)
        .output()
        .await
        .context("failed to run test command")?;

    let test_name = test_recipe.test_name();
    let stdout_tag = format!("[{}:stdout] ", test_name);
    let stderr_tag = format!("[{}:stderr] ", test_name);

    Ok(IntegrationTestResult {
        _exit_status_code: output.status.code(),
        success: output.status.success(),
        output: Output {
            stdout: Some(pretty_output(&output.stdout, &stdout_tag)),
            stderr: Some(pretty_output(&output.stderr, &stderr_tag)),
        },
    })
}

async fn run_graph_node(recipe: &IntegrationTestRecipe) -> anyhow::Result<Child> {
    use std::process::Stdio;

    let mut command = Command::new(recipe.graph_node_bin.as_os_str());
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--postgres-url")
        .arg(&recipe.postgres_uri)
        .arg("--ethereum-rpc")
        .arg(&recipe.ganache_uri)
        .arg("--ipfs")
        .arg(&recipe.ipfs_uri)
        .arg("--http-port")
        .arg(recipe.graph_node_ports.http.to_string())
        .arg("--index-node-port")
        .arg(recipe.graph_node_ports.index.to_string())
        .arg("--ws-port")
        .arg(recipe.graph_node_ports.ws.to_string())
        .arg("--admin-port")
        .arg(recipe.graph_node_ports.admin.to_string())
        .arg("--metrics-port")
        .arg(recipe.graph_node_ports.metrics.to_string());

    command
        .spawn()
        .context("failed to start graph-node command.")
}

async fn stop_graph_node(child: &mut Child) -> anyhow::Result<Output> {
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

    Ok(Output { stdout, stderr })
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
