use anyhow::Result;

use crate::{output, Ctx};

#[derive(clap::Args)]
pub struct Args {}

pub async fn command(_args: Args, ctx: &Ctx) -> Result<()> {
    let me = ctx.api()?.get("/api/v1/users/me", &Vec::new()).await?;
    if ctx.json {
        output::print_json(&me);
        return Ok(());
    }
    println!("{} ({})", output::field(&me, "email"), output::field(&me, "username"));
    println!("id: {}", output::field(&me, "id"));
    if output::field(&me, "isAdmin") == "true" {
        println!("role: admin");
    }
    Ok(())
}
