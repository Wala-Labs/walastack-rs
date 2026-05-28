//! WalaStack CLI binary — `walastack`.
//!
//! Phase 1 supports:
//! - `walastack new <name>` — scaffold a new project from the `basic-web` template
//! - `walastack dev` — run the project in development mode
//! - `walastack build [--release]` — build the project
//! - `walastack test` — run project tests
//!
//! Later phases add `deploy`, `logs`, `ai`, `sync`, `doctor`, etc.

#![allow(clippy::print_stdout)]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "walastack")]
#[command(about = "WalaStack — composable infrastructure for resilient and intelligent systems")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scaffold a new WalaStack project.
    New {
        /// Project name (becomes the directory name and Cargo package name).
        name: String,
        /// Template to use (default: `basic-web`).
        #[arg(long, default_value = "basic-web")]
        template: String,
    },
    /// Run the project in development mode (`cargo run` with `RUST_LOG=info`).
    Dev,
    /// Build the project.
    Build {
        /// Build in release mode.
        #[arg(long)]
        release: bool,
    },
    /// Run project tests.
    Test,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::New { name, template } => new_project(&name, &template),
        Commands::Dev => dev(),
        Commands::Build { release } => build(release),
        Commands::Test => test(),
    }
}

fn new_project(name: &str, template: &str) -> ExitCode {
    if let Err(reason) = validate_name(name) {
        eprintln!("error: invalid project name: {reason}");
        return ExitCode::FAILURE;
    }

    if template != "basic-web" {
        eprintln!("error: unknown template '{template}'. Available: basic-web");
        return ExitCode::FAILURE;
    }

    let project_dir = PathBuf::from(name);
    if project_dir.exists() {
        eprintln!("error: directory '{name}' already exists");
        return ExitCode::FAILURE;
    }

    if let Err(e) = scaffold_basic_web(&project_dir, name) {
        eprintln!("error: failed to scaffold project: {e}");
        return ExitCode::FAILURE;
    }

    println!("Created new WalaStack project: {name}");
    println!();
    println!("  cd {name}");
    println!("  walastack dev");
    println!();
    println!("Then open http://127.0.0.1:3000");

    ExitCode::SUCCESS
}

fn validate_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("project name cannot be empty");
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("project name cannot be empty");
    };
    if !first.is_ascii_alphabetic() {
        return Err("project name must start with a letter");
    }
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
            return Err("project name can only contain letters, digits, hyphens, and underscores");
        }
    }
    Ok(())
}

fn scaffold_basic_web(dir: &Path, name: &str) -> std::io::Result<()> {
    std::fs::create_dir(dir)?;
    std::fs::create_dir(dir.join("src"))?;

    let cargo_toml = include_str!("templates/basic-web/Cargo.toml.tpl").replace("{{name}}", name);
    std::fs::write(dir.join("Cargo.toml"), cargo_toml)?;

    let main_rs = include_str!("templates/basic-web/main.rs.tpl").replace("{{name}}", name);
    std::fs::write(dir.join("src/main.rs"), main_rs)?;

    let readme = include_str!("templates/basic-web/README.md.tpl").replace("{{name}}", name);
    std::fs::write(dir.join("README.md"), readme)?;

    std::fs::write(dir.join(".gitignore"), "/target\n")?;

    Ok(())
}

fn dev() -> ExitCode {
    let mut cmd = Command::new("cargo");
    cmd.arg("run");
    if std::env::var_os("RUST_LOG").is_none() {
        cmd.env("RUST_LOG", "info");
    }
    run_cargo_command(&mut cmd)
}

fn build(release: bool) -> ExitCode {
    let mut cmd = Command::new("cargo");
    cmd.arg("build");
    if release {
        cmd.arg("--release");
    }
    run_cargo_command(&mut cmd)
}

fn test() -> ExitCode {
    let mut cmd = Command::new("cargo");
    cmd.arg("test");
    run_cargo_command(&mut cmd)
}

fn run_cargo_command(cmd: &mut Command) -> ExitCode {
    match cmd.status() {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: failed to invoke cargo: {e}");
            eprintln!("       is cargo installed and on your PATH?");
            ExitCode::FAILURE
        }
    }
}
