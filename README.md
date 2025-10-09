# Enterprise Sobha Partner Portal Scraper

**Version: 1.0.0 | Status: Production Ready | For: BARACA Life Capital Real Estate**

This repository contains an enterprise-grade web scraper designed to extract property data from the Sobha Partner Portal. It is built for  with a focus on reliability, security, monitoring, and scalability.

## Table of Contents

- [Features](#features)
- [Technology Stack](#technology-stack)
- [Installation and Setup](#installation-and-setup)
- [Running the Actor](#running-the-actor)
- [Input Configuration](#input-configuration)
- [Output Structure](#output-structure)
- [Development](#development)
- [Support and Contribution](#support-and-contribution)

## Features

-   **Enterprise Reliability**: Comprehensive error handling, automatic retries with exponential backoff, and session management.
-   **Enhanced Security**: Stealth mode to bypass anti-bot detection, secure handling of credentials, and input sanitization.
-   **Performance Monitoring**: Detailed metrics collection for success rate, request times, and memory usage.
-   **Structured Logging**: Production-ready JSON-formatted logs for easy parsing and monitoring.
-   **Scalability**: Configurable concurrency to handle large-scale scraping tasks efficiently.
-   **Data Validation**: Robust validation of both input configuration and extracted data to ensure quality.

## Technology Stack

-   **[Apify SDK](https://docs.apify.com/sdk/js)**: Platform for building, running, and scaling web scrapers.
-   **[Crawlee](https://crawlee.dev)**: The web scraping and browser automation library.
-   **[Playwright](https://playwright.dev)**: For reliable, headless browser automation.
-   **Node.js**: The runtime environment.

## Installation and Setup

1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/No-Brainer-HQ/sobha-enterprise-scraper.git
    cd sobha-enterprise-scraper
    ```
2.  **Install Dependencies**:
    ```bash
    npm install
    ```
3.  **Apify Login**:
    If developing locally, log in to your Apify account.
    ```bash
    npx apify login
    ```

## Running the Actor

This Actor is designed to be run on the Apify platform. After linking this GitHub repository to your Apify account, you can run it directly from the Apify Console.

To run locally for development, use the Apify CLI:
```bash
npx apify run
```

## Input Configuration

The actor requires the following input configuration (defined in `INPUT_SCHEMA.json`):

| Field            | Type    | Required | Description                                       |
| ---------------- | ------- | -------- | ------------------------------------------------- |
| `email`          | String  | Yes      | Your Sobha Partner Portal login email.            |
| `password`       | Secret  | Yes      | Your Sobha Partner Portal login password.         |
| `scrapeMode`     | String  | No       | `bulk` (default) or `specific`.                   |
| `filters`        | Object  | No       | JSON object with filters like `bedrooms`, `project`. |
| `maxResults`     | Integer | No       | Maximum properties to scrape (default: 1000).     |
| `parallelRequests` | Integer | No       | Number of parallel browsers (default: 3).         |
| `enableStealth`  | Boolean | No       | Enable anti-detection measures (default: true).   |

## Output Structure

The scraper outputs a single JSON object to the dataset per run, containing detailed metrics, metadata, and the list of scraped properties.

```json
{
  "sessionId": "...",
  "timestamp": "...",
  "summary": {
    "totalProperties": 150,
    "successRate": 100
  },
  "properties": [
    {
      "unitId": "...",
      "project": "Sobha SeaHaven",
      "unitNo": "SSH-A4105",
      "startingPrice": "5,178,448",
      ...
    }
  ],
  "metrics": { ... }
}
```

## Development

Use the following npm scripts for local development:

-   `npm run dev`: Run the scraper with hot-reloading.
-   `npm run test`: Run automated tests.
-   `npm run lint`: Lint the codebase.
-   `npm run security-audit`: Check for vulnerabilities.

## Support and Contribution

This is a proprietary project for BARACA. For support, issues, or feature requests, please contact the BARACA Engineering Team.
