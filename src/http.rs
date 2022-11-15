use crate::Error;
use indicatif::{MultiProgress, ProgressBar};

const RETRIES: u32 = 10;

pub fn fetch_url_data(source: &str) -> Result<Box<[u8]>, Error> {
    let mut data = Vec::new();
    retry(|| fetch(source, |pb| pb, |_| {}, &mut data), RETRIES).map(|()| data.into_boxed_slice())
}

pub fn fetch_url_data_with_progress(
    source: &str,
    progress: &MultiProgress,
) -> Result<Box<[u8]>, Error> {
    let mut data = Vec::new();
    retry(
        || {
            fetch(
                source,
                |pb| progress.add(pb),
                |pb| progress.remove(pb),
                &mut data,
            )
        },
        RETRIES,
    )
    .map(|()| data.into_boxed_slice())
}

fn fetch<A, R>(source: &str, add_bar: A, remove_bar: R, zip_data: &mut Vec<u8>) -> Result<(), Error>
where
    A: FnOnce(ProgressBar) -> ProgressBar,
    R: FnOnce(&ProgressBar),
{
    use attohttpc::header::CONTENT_LENGTH;
    use std::io::Read;

    let builder = if zip_data.is_empty() {
        attohttpc::get(source)
    } else {
        attohttpc::get(source).header("Range", format!("bytes={}-", zip_data.len()))
    };

    match builder.send()?.split() {
        (code, map, reader) if code.is_success() => {
            let length = map
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            let pbar = add_bar(progress_bar(source, length));

            let result = pbar
                .wrap_read(reader)
                .read_to_end(zip_data)
                .map(|_| ())
                .map_err(Error::IO);

            remove_bar(&pbar);

            result
        }
        (code, _, _) => Err(Error::HttpCode(code)),
    }
}

#[inline]
fn retry<T, E, F>(mut f: F, mut retries: u32) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
{
    loop {
        match f() {
            ok @ Ok(_) => break ok,
            err @ Err(_) if retries == 0 => break err,
            Err(_) => retries -= 1,
        }
    }
}

fn progress_bar(source: &str, total_bytes: Option<u64>) -> ProgressBar {
    use indicatif::ProgressStyle;

    match total_bytes {
        Some(total_bytes) => ProgressBar::new(total_bytes).with_style(
            ProgressStyle::default_bar()
                .template("{wide_msg} {bytes} ({bytes_per_sec}) {eta}")
                .unwrap(),
        ),
        None => ProgressBar::new_spinner().with_style(
            ProgressStyle::default_spinner()
                .template("{wide_msg} {bytes} ({bytes_per_sec})")
                .unwrap(),
        ),
    }
    .with_message(source.to_owned())
}
