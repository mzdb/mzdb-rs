// little anyhow extension to annotate errors with line numbers (see https://github.com/dtolnay/anyhow/issues/22)

use anyhow::{Context, Result};
use std::fmt::Display;

pub struct Location {
    pub file: &'static str,
    pub line: u32,
    pub column: u32,
}

pub trait ErrorLocation<T, E> {
    fn location(self, loc: &'static Location) -> Result<T>;
}

impl<T, E> ErrorLocation<T, E> for Result<T, E>
    where
        E: Display,
        Result<T, E>: Context<T, E>,
{
    fn location(self, loc: &'static Location) -> Result<T> {
        let msg = self.as_ref().err().map(ToString::to_string);
        self.with_context(|| format!(
            "{} at {} line {} column {}",
            msg.unwrap(), loc.file, loc.line, loc.column,
        ))
    }
}

#[macro_export]
macro_rules! here {
    () => {
        &Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}

// See: https://stackoverflow.com/questions/26731243/how-do-i-use-a-macro-across-module-files
pub(crate) use here;    // <-- the trick
