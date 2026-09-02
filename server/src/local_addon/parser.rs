use regex::Regex;
use std::path::Path;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct VideoMetadata {
    pub name: Option<String>,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<Vec<i32>>,
    pub disk_number: Option<i32>,
    pub type_: String,
    pub imdb_id: Option<String>,
    pub tags: Vec<String>,
}

fn get_extensions() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\.(mkv|avi|mp4|wmv|vp8|mov|mpg|mp3|flac)$").unwrap())
}

fn get_movie_keywords() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)(1080p|720p|480p|blurayrip|brrip|divx|dvdrip|hdrip|hdtv|tvrip|xvid|camrip)",
        )
        .unwrap()
    })
}

fn get_season_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)S(\d{1,2})").unwrap())
}

fn get_episode_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)E(\d{2})").unwrap())
}

fn get_year_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b(19|20)\d{2}\b").unwrap())
}

fn get_sample_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)(sample|etrg)").unwrap())
}

fn has_episode_prefix(text: &str, episode_start: usize) -> bool {
    text[..episode_start]
        .chars()
        .next_back()
        .is_some_and(|ch| !ch.is_alphanumeric() || ch.is_numeric())
}

fn has_episode_marker(text: &str) -> bool {
    get_episode_regex().captures_iter(text).any(|caps| {
        caps.get(0)
            .is_some_and(|m| has_episode_prefix(text, m.start()))
    })
}

pub fn parse_filename(path: &Path) -> Option<VideoMetadata> {
    let filename = path.file_name()?.to_str()?;
    if !get_extensions().is_match(filename) {
        return None;
    }

    let mut meta = VideoMetadata {
        type_: "other".to_string(),
        ..VideoMetadata::default()
    };

    let clean_name = filename.replace(['.', '_', '-'], " ");

    if let Some(caps) = get_year_regex().find(&clean_name)
        && let Ok(year) = caps.as_str().parse::<i32>()
    {
        meta.year = Some(year);
    }

    if let Some(caps) = get_season_regex().captures(&clean_name)
        && let Some(s) = caps.get(1)
    {
        meta.season = s.as_str().parse::<i32>().ok();
    }

    let mut episodes = Vec::new();
    for caps in get_episode_regex().captures_iter(&clean_name) {
        let Some(episode_match) = caps.get(0) else {
            continue;
        };
        if !has_episode_prefix(&clean_name, episode_match.start()) {
            continue;
        }
        if let Some(e) = caps.get(1)
            && let Ok(ep) = e.as_str().parse::<i32>()
        {
            episodes.push(ep);
        }
    }
    if !episodes.is_empty() {
        meta.episode = Some(episodes);
    }

    // Determine Type
    if meta.season.is_some() && meta.episode.is_some() {
        meta.type_ = "series".to_string();
    } else if meta.year.is_some() || get_movie_keywords().is_match(&clean_name) {
        meta.type_ = "movie".to_string();
    }

    let parts: Vec<&str> = clean_name.split_whitespace().collect();
    let mut name_parts = Vec::new();
    for part in parts {
        if get_year_regex().is_match(part)
            || get_season_regex().is_match(part)
            || has_episode_marker(part)
            || get_movie_keywords().is_match(part)
        {
            break;
        }
        name_parts.push(part);
    }

    if !name_parts.is_empty() {
        meta.name = Some(name_parts.join(" "));
    } else {
        meta.name = Some(
            path.file_stem()?
                .to_str()?
                .to_string()
                .replace(['.', '_'], " "),
        );
    }

    if clean_name.contains("1080p") {
        meta.tags.push("1080p".to_string());
        meta.tags.push("hd".to_string());
    }
    if clean_name.contains("720p") {
        meta.tags.push("720p".to_string());
    }
    if clean_name.contains("480p") {
        meta.tags.push("480p".to_string());
    }
    if get_sample_regex().is_match(&clean_name) {
        meta.tags.push("sample".to_string());
    }

    Some(meta)
}

#[cfg(test)]
mod tests {
    use super::parse_filename;
    use std::path::Path;

    #[test]
    fn parses_compact_season_episode() {
        let meta = parse_filename(Path::new("Some.Show.S01E02.mkv")).unwrap();

        assert_eq!(meta.name.as_deref(), Some("Some Show"));
        assert_eq!(meta.season, Some(1));
        assert_eq!(meta.episode, Some(vec![2]));
        assert_eq!(meta.type_, "series");
    }

    #[test]
    fn preserves_leading_episode_marker_behavior() {
        let meta = parse_filename(Path::new("E02.Some.Show.mkv")).unwrap();

        assert_eq!(meta.episode, None);
    }

    #[test]
    fn parses_multi_episode_file() {
        let meta = parse_filename(Path::new("Show.S01E01E02.mkv")).unwrap();

        assert_eq!(meta.season, Some(1));
        assert_eq!(meta.episode, Some(vec![1, 2]));
        assert_eq!(meta.type_, "series");
    }

    #[test]
    fn episode_marker_glued_to_letters_is_not_an_episode() {
        // The E-marker only counts when preceded by a non-alphanumeric or a
        // digit; a letter prefix (codec/resolution noise) must not register.
        let meta = parse_filename(Path::new("CoreE05.mkv")).unwrap();

        assert_eq!(meta.episode, None);
        assert_ne!(meta.type_, "series");
    }

    #[test]
    fn classifies_series_movie_and_other() {
        let series = parse_filename(Path::new("Show.S02E03.mkv")).unwrap();
        assert_eq!(series.type_, "series");

        let movie_by_year = parse_filename(Path::new("Some.Movie.2019.mkv")).unwrap();
        assert_eq!(movie_by_year.type_, "movie");
        assert_eq!(movie_by_year.year, Some(2019));

        let movie_by_keyword = parse_filename(Path::new("Some.Movie.1080p.mkv")).unwrap();
        assert_eq!(movie_by_keyword.type_, "movie");
        assert_eq!(movie_by_keyword.year, None);

        let other = parse_filename(Path::new("randomclip.mkv")).unwrap();
        assert_eq!(other.type_, "other");
    }

    #[test]
    fn truncates_name_before_first_tag_token_and_collects_tags() {
        let meta = parse_filename(Path::new("Some.Show.2019.1080p.mkv")).unwrap();

        assert_eq!(meta.name.as_deref(), Some("Some Show"));
        assert_eq!(meta.year, Some(2019));
        assert!(meta.tags.contains(&"1080p".to_string()));
        assert!(meta.tags.contains(&"hd".to_string()));
    }

    #[test]
    fn non_media_extension_returns_none() {
        assert!(parse_filename(Path::new("movie.txt")).is_none());
        assert!(parse_filename(Path::new("archive.zip")).is_none());
    }
}
