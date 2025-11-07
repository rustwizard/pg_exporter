use std::process::Command;

fn main() {
    // Get the Git hash
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output();

    let git_hash = match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => {
            eprintln!(
                "Warning: Could not get Git hash. Is Git installed and are you in a Git repository?"
            );
            "unknown".to_string()
        }
    };

    // Make the hash available as an environment variable
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Rerun if the HEAD of the Git repository changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index"); // Also consider index changes
}
