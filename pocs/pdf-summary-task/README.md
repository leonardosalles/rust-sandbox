# PDF Summary Task

A Rust application that processes PDF files from S3, extracts text, generates summaries, and saves the results back to S3.

## How it works

1. Reads S3 URI from `S3_INPUT_URI` environment variable
2. Reads S3 output URI from `S3_OUTPUT_URI` environment variable
3. Downloads the PDF file from the input S3 location
4. Extracts text from the PDF (currently mocked)
5. Summarizes the extracted text (currently mocked)
6. Saves the summary to the output S3 location in JSON format following the task definition schema

## Environment Variables

- `S3_INPUT_URI`: Input S3 URI for the PDF file (e.g., `s3://bucket/path/to/file.pdf`)
- `S3_OUTPUT_URI`: Output S3 URI for the summary JSON (e.g., `s3://bucket/path/to/output.json`)

## Running

```bash
export S3_INPUT_URI="s3://your-bucket/input.pdf"
export S3_OUTPUT_URI="s3://your-bucket/output.json"
cargo run
```

## Output Schema

The output follows the schema defined in `task.definition.json`:

```json
{
  "summary": "string"
}
```

## Mock Implementation

Currently both PDF text extraction and summarization are mocked for testing purposes. In a production environment, you would replace these with actual implementations using libraries like:

- PDF extraction: `pdf-extract`, `lopdf`, or similar
- Summarization: OpenAI API, local LLM, or other AI services

## AWS Configuration

Ensure your AWS credentials are properly configured in the environment or through `~/.aws/credentials` for S3 access.
