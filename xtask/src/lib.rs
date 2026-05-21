use std::fmt;
use std::process::{Command, ExitStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Task {
    Qa,
    MaintainerQa,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    pub name: &'static str,
    pub args: &'static [&'static str],
    pub env: &'static [(&'static str, &'static str)],
}

const FMT_STEP: Step = Step {
    name: "fmt",
    args: &["fmt", "--check"],
    env: &[],
};

const CLIPPY_STEP: Step = Step {
    name: "clippy",
    args: &[
        "clippy",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
    ],
    env: &[],
};

const TEST_STEP: Step = Step {
    name: "test",
    args: &["test", "--workspace", "--all-targets", "--all-features"],
    env: &[],
};

const LOOM_STEP: Step = Step {
    name: "loom",
    args: &["test", "--workspace", "--features", "loom"],
    env: &[],
};

const FASTEMBED_BUILD_STEP: Step = Step {
    name: "fastembed-build",
    args: &["build", "--features", "fastembed"],
    env: &[],
};

const FASTEMBED_TEST_STEP: Step = Step {
    name: "fastembed-test",
    args: &[
        "test",
        "--workspace",
        "--all-targets",
        "--features",
        "fastembed",
    ],
    env: &[],
};

const DOC_STEP: Step = Step {
    name: "doc",
    args: &["doc", "--workspace", "--no-deps"],
    env: &[("RUSTDOCFLAGS", "-D warnings")],
};

const DENY_STEP: Step = Step {
    name: "deny",
    args: &["deny", "check"],
    env: &[],
};

const AUDIT_STEP: Step = Step {
    name: "audit",
    args: &["audit"],
    env: &[],
};

const QA_STEPS: [Step; 3] = [FMT_STEP, CLIPPY_STEP, TEST_STEP];

const MAINTAINER_QA_STEPS: [Step; 9] = [
    FMT_STEP,
    CLIPPY_STEP,
    TEST_STEP,
    LOOM_STEP,
    FASTEMBED_BUILD_STEP,
    FASTEMBED_TEST_STEP,
    DOC_STEP,
    DENY_STEP,
    AUDIT_STEP,
];

#[derive(Debug)]
pub enum XtaskError {
    UnknownCommand {
        command: String,
    },
    SpawnFailed {
        step: &'static str,
        source: std::io::Error,
    },
    StepFailed {
        step: &'static str,
        status: ExitStatus,
    },
}

impl fmt::Display for XtaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCommand { command } => {
                write!(
                    f,
                    "unknown xtask command: {command} (supported: qa, maintainer-qa)"
                )
            }
            Self::SpawnFailed { step, source } => {
                write!(f, "failed to spawn cargo step `{step}`: {source}")
            }
            Self::StepFailed { step, status } => {
                write!(f, "cargo step `{step}` failed with status {status}")
            }
        }
    }
}

impl std::error::Error for XtaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SpawnFailed { source, .. } => Some(source),
            Self::UnknownCommand { .. } | Self::StepFailed { .. } => None,
        }
    }
}

pub fn parse_task(args: &[String]) -> Result<Task, XtaskError> {
    match args.first().map(String::as_str) {
        None | Some("qa") => Ok(Task::Qa),
        Some("maintainer-qa") => Ok(Task::MaintainerQa),
        Some(command) => Err(XtaskError::UnknownCommand {
            command: command.to_string(),
        }),
    }
}

pub fn qa_steps() -> &'static [Step] {
    &QA_STEPS
}

pub fn maintainer_qa_steps() -> &'static [Step] {
    &MAINTAINER_QA_STEPS
}

pub fn run_task(task: Task) -> Result<(), XtaskError> {
    let steps = match task {
        Task::Qa => qa_steps(),
        Task::MaintainerQa => maintainer_qa_steps(),
    };

    for step in steps {
        run_step(step)?;
    }

    Ok(())
}

fn run_step(step: &Step) -> Result<(), XtaskError> {
    println!("==> cargo {}", step.args.join(" "));

    let mut command = Command::new("cargo");
    command.args(step.args);
    command.envs(step.env.iter().copied());

    let status = command.status().map_err(|source| XtaskError::SpawnFailed {
        step: step.name,
        source,
    })?;

    if status.success() {
        Ok(())
    } else {
        Err(XtaskError::StepFailed {
            step: step.name,
            status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Task, maintainer_qa_steps, parse_task, qa_steps};

    #[test]
    fn qa_steps_match_local_developer_gate() {
        let steps = qa_steps();

        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].name, "fmt");
        assert_eq!(steps[0].args, ["fmt", "--check"]);
        assert_eq!(steps[1].name, "clippy");
        assert_eq!(
            steps[1].args,
            [
                "clippy",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--",
                "-D",
                "warnings",
            ]
        );
        assert_eq!(steps[2].name, "test");
        assert_eq!(
            steps[2].args,
            ["test", "--workspace", "--all-targets", "--all-features"]
        );
    }

    #[test]
    fn parse_task_defaults_to_qa() {
        assert_eq!(parse_task(&[]).expect("task"), Task::Qa);
    }

    #[test]
    fn parse_task_accepts_explicit_qa() {
        assert_eq!(parse_task(&["qa".to_string()]).expect("task"), Task::Qa);
    }

    #[test]
    fn parse_task_accepts_explicit_maintainer_qa() {
        assert_eq!(
            parse_task(&["maintainer-qa".to_string()]).expect("task"),
            Task::MaintainerQa
        );
    }

    #[test]
    fn parse_task_rejects_unknown_subcommand() {
        let err = parse_task(&["unknown".to_string()]).expect_err("unknown command");
        assert!(err.to_string().contains("supported: qa, maintainer-qa"));
    }

    #[test]
    fn maintainer_qa_steps_match_local_maintainer_gate() {
        let steps = maintainer_qa_steps();

        assert_eq!(steps.len(), 9);
        assert_eq!(steps[0], qa_steps()[0]);
        assert_eq!(steps[1], qa_steps()[1]);
        assert_eq!(steps[2], qa_steps()[2]);
        assert_eq!(steps[3].name, "loom");
        assert_eq!(steps[3].args, ["test", "--workspace", "--features", "loom"]);
        assert_eq!(steps[4].name, "fastembed-build");
        assert_eq!(steps[4].args, ["build", "--features", "fastembed"]);
        assert_eq!(steps[5].name, "fastembed-test");
        assert_eq!(
            steps[5].args,
            [
                "test",
                "--workspace",
                "--all-targets",
                "--features",
                "fastembed"
            ]
        );
        assert_eq!(steps[6].name, "doc");
        assert_eq!(steps[6].args, ["doc", "--workspace", "--no-deps"]);
        assert_eq!(steps[6].env, [("RUSTDOCFLAGS", "-D warnings")]);
        assert_eq!(steps[7].name, "deny");
        assert_eq!(steps[7].args, ["deny", "check"]);
        assert_eq!(steps[8].name, "audit");
        assert_eq!(steps[8].args, ["audit"]);
    }
}
