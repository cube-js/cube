use minijinja as mj;
use std::error::Error;

use neon::prelude::*;

pub(crate) trait NeonMiniJinjaContext<'a>: Context<'a> {
    fn throw_from_mj_error<T>(&mut self, err: mj::Error) -> NeonResult<T> {
        let codeblock = if let Some(source) = err.template_source() {
            let lines: Vec<_> = source.lines().enumerate().collect();
            let idx = err.line().unwrap_or(1).saturating_sub(1);
            let skip = idx.saturating_sub(3);

            let pre = lines.iter().skip(skip).take(3.min(idx)).collect::<Vec<_>>();
            let post = lines.iter().skip(idx + 1).take(3).collect::<Vec<_>>();

            let mut content = "".to_string();

            for (idx, line) in pre {
                content += &format!("{:>4} | {}\r\n", idx + 1, line);
            }

            content += &format!("{:>4} > {}\r\n", idx + 1, lines[idx].1);

            if let Some(_span) = err.range() {
                // TODO(ovr): improve
                content += &format!(
                    "     i {}{} {}\r\n",
                    " ".repeat(0),
                    "^".repeat(24),
                    err.kind(),
                );
            } else {
                content += &format!("     | {}\r\n", "^".repeat(24));
            }

            for (idx, line) in post {
                content += &format!("{:>4} | {}\r\n", idx + 1, line);
            }

            format!("{}\r\n{}\r\n{}", "-".repeat(79), content, "-".repeat(79))
        } else {
            "".to_string()
        };

        if let Some(next_err) = err.source() {
            self.throw_error(format!(
                "{} caused by: {:#}\r\n{}",
                err, next_err, codeblock
            ))
        } else {
            self.throw_error(format!("{}\r\n{}", err, codeblock))
        }
    }
}

impl<'a> NeonMiniJinjaContext<'a> for FunctionContext<'a> {}

impl<'a> NeonMiniJinjaContext<'a> for TaskContext<'a> {}
