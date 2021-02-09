pub enum MaybeOwnedStr<'a> {
    Owned(String),
    Borrowed(&'a str),
}

impl AsRef<str> for MaybeOwnedStr<'_> {
    fn as_ref(&self) -> &str {
        match self {
            MaybeOwnedStr::Owned(s) => &s,
            MaybeOwnedStr::Borrowed(s) => s,
        }
    }
}

impl MaybeOwnedStr<'a> {
    pub fn take_string(self) -> String {
        match self {
            MaybeOwnedStr::Owned(s) => s,
            MaybeOwnedStr::Borrowed(s) => s.to_string(),
        }
    }
}
