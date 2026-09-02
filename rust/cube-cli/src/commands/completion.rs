use anyhow::Result;
use clap_complete::Shell;

#[derive(clap::Args)]
pub struct Args {
    /// Shell to generate completions for
    #[arg(value_enum)]
    shell: Shell,
}

pub fn command(args: Args) -> Result<()> {
    let mut cmd = crate::cli_command();
    clap_complete::generate(args.shell, &mut cmd, "cube", &mut std::io::stdout());
    Ok(())
}
