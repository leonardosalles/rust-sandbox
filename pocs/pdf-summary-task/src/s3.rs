use aws_sdk_s3::Client;

pub async fn download_s3_file(
    uri: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let (bucket, key) = parse_s3_uri(uri)?;

    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    let obj = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let data = obj.body.collect().await?;
    Ok(data.into_bytes().to_vec())
}

pub async fn upload_s3_file(
    uri: &str,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let (bucket, key) = parse_s3_uri(uri)?;

    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(data.into())
        .send()
        .await?;

    Ok(())
}

fn parse_s3_uri(uri: &str) -> Result<(&str, &str), Box<dyn std::error::Error>> {
    let trimmed = uri.strip_prefix("s3://")
        .ok_or("invalid s3 uri")?;

    let parts: Vec<&str> = trimmed.splitn(2, '/').collect();

    if parts.len() != 2 || parts[1].is_empty() {
        return Err("invalid s3 uri format".into());
    }

    Ok((parts[0], parts[1]))
}
