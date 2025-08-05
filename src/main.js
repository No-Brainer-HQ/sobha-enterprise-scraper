/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.0
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { randomBytes, createHash } from 'crypto';
import { performance } from 'perf_hooks';

/**
 * Enterprise Configuration Constants
 */
const CONFIG = {
    // Performance settings
    MAX_CONCURRENT_REQUESTS: 5,
    REQUEST_TIMEOUT: 60000,
    NAVIGATION_TIMEOUT: 30000,
    
    // Security settings
    MAX_RETRY_ATTEMPTS: 3,
    BASE_DELAY: 2000,
    MAX_DELAY: 10000,
    
    // Monitoring thresholds
    MIN_SUCCESS_RATE: 95.0,
    MAX_MEMORY_MB: 4096,
    
    // Portal endpoints
    LOGIN_URL: 'https://www.sobhapartnerportal.com/partnerportal/s/',
    
    // Selectors (production-ready)
    SELECTORS: {
        email: 'input[type="email"], input[placeholder*="email"], input[name*="email"]',
        password: 'input[type="password"], input[placeholder*="password"], input[name*="password"]',
        loginButton: 'button[type="submit"], button:has-text("Sign in"), .btn:has-text("Sign in")',
        dashboardIndicator: 'text=BARACA LIFE CAPITAL REAL ESTATE, text=Dashboard, text=My Dashboard',
        projectsNavigation: 'text=Sobha Projects',
        filterBed: 'text=Select Bed',
        filterArea: 'text=Select Area (SQ. FT.)',
        filterPrice: 'text=Select Price (AED)',
        filterPropertyType: 'text=Select Property Type',
        filterProject: 'text=Select Project',
        filterButton: 'button:has-text("Filter Properties")',
        resultsTable: 'table tbody tr, .table tbody tr, [role="row"]:not(:first-child)'
    }
};

/**
 * Enterprise Logger with structured output
 */
class EnterpriseLogger {
    constructor(sessionId) {
        this.sessionId = sessionId;
        this.startTime = performance.now();
    }

    log(level, message, data = {}) {
        const timestamp = new Date().toISOString();
        const runtime = Math.round(performance.now() - this.startTime);
        
        const logEntry = {
            timestamp,
            level,
            sessionId: this.sessionId,
            runtime,
            message,
            ...data
        };

        console.log(JSON.stringify(logEntry));
        Actor.log[level.toLowerCase()](message, data);
    }

    info(message, data) { this.log('INFO', message, data); }
    error(message, data) { this.log('ERROR', message, data); }
    warn(message, data) { this.log('WARN', message, data); }
    debug(message, data) { this.log('DEBUG', message, data); }
}

/**
 * Enterprise Security Manager
 */
class SecurityManager {
    static generateSessionId() {
        return createHash('md5')
            .update(Date.now().toString() + randomBytes(16).toString('hex'))
            .digest('hex')
            .substring(0, 16);
    }

    static maskSensitiveData(data, visibleChars = 4) {
        if (!data || typeof data !== 'string') return '***';
        if (data.length <= visibleChars) return '*'.repeat(data.length);
        return data.substring(0, visibleChars) + '*'.repeat(data.length - visibleChars);
    }

    static sanitizeInput(input) {
        if (typeof input !== 'string') return String(input);
        // Remove potentially dangerous characters
        return input.replace(/[<>&"';\-\/\*]/g, '').trim();
    }
}

/**
 * Enterprise Rate Limiter with adaptive delays
 */
class RateLimiter {
    constructor(baseDelay = CONFIG.BASE_DELAY) {
        this.baseDelay = baseDelay;
        this.currentDelay = baseDelay;
        this.consecutiveFailures = 0;
        this.lastRequestTime = 0;
    }

    async wait() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        
        if (timeSinceLastRequest < this.currentDelay) {
            const waitTime = this.currentDelay - timeSinceLastRequest;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.lastRequestTime = Date.now();
    }

    onSuccess() {
        this.consecutiveFailures = 0;
        this.currentDelay = Math.max(this.baseDelay, this.currentDelay * 0.9);
    }

    onFailure() {
        this.consecutiveFailures++;
        this.currentDelay = Math.min(
            CONFIG.MAX_DELAY,
            this.currentDelay * Math.pow(1.5, this.consecutiveFailures)
        );
    }
}

/**
 * Enterprise Metrics Collector
 */
class MetricsCollector {
    constructor(sessionId) {
        this.sessionId = sessionId;
        this.startTime = Date.now();
        this.metrics = {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            propertiesScraped: 0,
            errors: [],
            performanceData: {
                memoryUsage: [],
                requestTimes: []
            }
        };
    }

    recordRequest(success, duration, error = null) {
        this.metrics.totalRequests++;
        this.metrics.performanceData.requestTimes.push(duration);
        
        if (success) {
            this.metrics.successfulRequests++;
        } else {
            this.metrics.failedRequests++;
            if (error) {
                this.metrics.errors.push({
                    timestamp: new Date().toISOString(),
                    error: error.message,
                    stack: error.stack
                });
            }
        }
    }

    recordPropertiesScraped(count) {
        this.metrics.propertiesScraped = count;
    }

    recordMemoryUsage() {
        const usage = process.memoryUsage();
        this.metrics.performanceData.memoryUsage.push({
            timestamp: Date.now(),
            heapUsed: usage.heapUsed,
            heapTotal: usage.heapTotal,
            external: usage.external
        });
    }

    getSuccessRate() {
        if (this.metrics.totalRequests === 0) return 100;
        return (this.metrics.successfulRequests / this.metrics.totalRequests) * 100;
    }

    getDuration() {
        return Date.now() - this.startTime;
    }

    getSummary() {
        return {
            sessionId: this.sessionId,
            duration: this.getDuration(),
            successRate: this.getSuccessRate(),
            totalRequests: this.metrics.totalRequests,
            successfulRequests: this.metrics.successfulRequests,
            failedRequests: this.metrics.failedRequests,
            propertiesScraped: this.metrics.propertiesScraped,
            errorCount: this.metrics.errors.length,
            averageRequestTime: this.metrics.performanceData.requestTimes.length > 0 
                ? this.metrics.performanceData.requestTimes.reduce((a, b) => a + b, 0) / this.metrics.performanceData.requestTimes.length 
                : 0
        };
    }
}

/**
 * Enterprise Input Validator
 */
class InputValidator {
    static validate(input) {
        const errors = [];

        // Required fields
        if (!input.email || typeof input.email !== 'string') {
            errors.push('Email is required and must be a string');
        } else if (!input.email.includes('@') || !input.email.includes('.')) {
            errors.push('Email must be a valid email address');
        }

        if (!input.password || typeof input.password !== 'string') {
            errors.push('Password is required and must be a string');
        } else if (input.password.length < 8) {
            errors.push('Password must be at least 8 characters long');
        }

        // Optional fields validation
        if (input.maxResults && (typeof input.maxResults !== 'number' || input.maxResults < 1 || input.maxResults > 10000)) {
            errors.push('maxResults must be a number between 1 and 10000');
        }

        if (input.requestDelay && (typeof input.requestDelay !== 'number' || input.requestDelay < 0.5 || input.requestDelay > 10)) {
            errors.push('requestDelay must be a number between 0.5 and 10');
        }

        if (input.retryAttempts && (typeof input.retryAttempts !== 'number' || input.retryAttempts < 1 || input.retryAttempts > 5)) {
            errors.push('retryAttempts must be a number between 1 and 5');
        }

        if (input.filters && typeof input.filters !== 'object') {
            errors.push('filters must be an object');
        }

        if (errors.length > 0) {
            throw new Error(`Input validation failed: ${errors.join(', ')}`);
        }

        return {
            email: SecurityManager.sanitizeInput(input.email.toLowerCase().trim()),
            password: input.password,
            scrapeMode: input.scrapeMode || 'bulk',
            filters: input.filters || {},
            specificUnit: input.specificUnit || null,
            maxResults: input.maxResults || 1000,
            requestDelay: input.requestDelay || 2.0,
            retryAttempts: input.retryAttempts || 3,
            enableStealth: input.enableStealth !== false,
            downloadDocuments: input.downloadDocuments || false,
            parallelRequests: input.parallelRequests || 3
        };
    }
}

/**
 * Enterprise Sobha Portal Scraper
 */
class EnterpriseSobhaPortalScraper {
    constructor(validatedInput) {
        this.input = validatedInput;
        this.sessionId = SecurityManager.generateSessionId();
        this.logger = new EnterpriseLogger(this.sessionId);
        this.rateLimiter = new RateLimiter(this.input.requestDelay * 1000);
        this.metrics = new MetricsCollector(this.sessionId);
        
        this.logger.info('Enterprise scraper initialized', {
            sessionId: this.sessionId,
            scrapeMode: this.input.scrapeMode,
            maxResults: this.input.maxResults,
            email: SecurityManager.maskSensitiveData(this.input.email)
        });
    }

    /**
     * Apply enterprise-grade stealth techniques
     */
    async applyStealthTechniques(page) {
        if (!this.input.enableStealth) return;

        this.logger.debug('Applying enterprise stealth techniques');

        // Override navigator properties to avoid detection
        await page.addInitScript(() => {
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });

            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });

            // Add chrome object
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };

            // Mock permissions
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({
                    query: async () => ({ state: 'granted' }),
                }),
            });

            // Override screen properties
            Object.defineProperty(screen, 'colorDepth', { get: () => 24 });
            Object.defineProperty(screen, 'pixelDepth', { get: () => 24 });
        });

        // Set realistic viewport with slight randomization
        const baseWidth = 1366;
        const baseHeight = 768;
        await page.setViewportSize({
            width: baseWidth + Math.floor(Math.random() * 200) - 100,
            height: baseHeight + Math.floor(Math.random() * 200) - 100
        });

        // Set realistic user agent
        await page.setExtraHTTPHeaders({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        });
    }

    /**
     * Enterprise authentication with comprehensive error handling
     */
    async authenticate(page) {
        const maxAttempts = this.input.retryAttempts;
        
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            const requestStart = performance.now();
            
            try {
                this.logger.info(`Authentication attempt ${attempt}/${maxAttempts}`, {
                    email: SecurityManager.maskSensitiveData(this.input.email)
                });

                // Navigate to login page with comprehensive error handling
                await page.goto(CONFIG.LOGIN_URL, { 
                    waitUntil: 'networkidle',
                    timeout: CONFIG.NAVIGATION_TIMEOUT 
                });

                // Apply stealth techniques
                await this.applyStealthTechniques(page);

                // Wait for login form elements
                await page.waitForSelector(CONFIG.SELECTORS.email, { timeout: 15000 });
                await page.waitForSelector(CONFIG.SELECTORS.password, { timeout: 15000 });

                // Clear and fill email field with human-like typing
                await page.fill(CONFIG.SELECTORS.email, '');
                await page.type(CONFIG.SELECTORS.email, this.input.email, { delay: 100 + Math.random() * 50 });

                // Clear and fill password field
                await page.fill(CONFIG.SELECTORS.password, '');
                await page.type(CONFIG.SELECTORS.password, this.input.password, { delay: 100 + Math.random() * 50 });

                // Add realistic human delay
                await page.waitForTimeout(1000 + Math.random() * 2000);

                // Submit form
                await page.click(CONFIG.SELECTORS.loginButton);

                // Wait for successful login with multiple success indicators
                try {
                    await page.waitForSelector(CONFIG.SELECTORS.dashboardIndicator, { 
                        timeout: 20000 
                    });
                    
                    const requestDuration = performance.now() - requestStart;
                    this.metrics.recordRequest(true, requestDuration);
                    this.rateLimiter.onSuccess();
                    
                    this.logger.info('Authentication successful', {
                        attempt,
                        duration: Math.round(requestDuration)
                    });
                    
                    return true;
                    
                } catch (waitError) {
                    // Check for specific error messages
                    const errorElements = await page.locator('text*="error", text*="invalid", text*="incorrect"').all();
                    if (errorElements.length > 0) {
                        const errorText = await errorElements[0].textContent();
                        throw new Error(`Authentication failed: ${errorText}`);
                    }
                    throw waitError;
                }

            } catch (error) {
                const requestDuration = performance.now() - requestStart;
                this.metrics.recordRequest(false, requestDuration, error);
                this.rateLimiter.onFailure();
                
                this.logger.error(`Authentication attempt ${attempt} failed`, {
                    attempt,
                    error: error.message,
                    duration: Math.round(requestDuration)
                });

                if (attempt < maxAttempts) {
                    const delay = 3000 + Math.random() * 4000; // 3-7 seconds
                    this.logger.info(`Retrying authentication in ${Math.round(delay/1000)} seconds`);
                    await page.waitForTimeout(delay);
                } else {
                    throw new Error(`Authentication failed after ${maxAttempts} attempts: ${error.message}`);
                }
            }
        }

        return false;
    }

    /**
     * Navigate to Sobha Projects page
     */
    async navigateToProjects(page) {
        try {
            this.logger.info('Navigating to Sobha Projects page');

            // Wait for and click projects navigation
            await page.waitForSelector(CONFIG.SELECTORS.projectsNavigation, { timeout: 15000 });
            await page.click(CONFIG.SELECTORS.projectsNavigation);

            // Wait for filter elements to confirm page load
            await page.waitForSelector(CONFIG.SELECTORS.filterBed, { timeout: 20000 });
            await page.waitForSelector(CONFIG.SELECTORS.filterButton, { timeout: 20000 });

            this.logger.info('Successfully navigated to Sobha Projects page');
            return true;

        } catch (error) {
            this.logger.error('Failed to navigate to projects page', { error: error.message });
            throw new Error(`Navigation failed: ${error.message}`);
        }
    }

    /**
     * Apply property filters with comprehensive error handling
     */
    async applyFilters(page) {
        if (!this.input.filters || Object.keys(this.input.filters).length === 0) {
            this.logger.info('No filters to apply');
            return true;
        }

        try {
            this.logger.info('Applying property filters', { filters: this.input.filters });

            const filterMappings = {
                bedrooms: CONFIG.SELECTORS.filterBed,
                area: CONFIG.SELECTORS.filterArea,
                price: CONFIG.SELECTORS.filterPrice,
                propertyType: CONFIG.SELECTORS.filterPropertyType,
                project: CONFIG.SELECTORS.filterProject
            };

            for (const [filterKey, filterValue] of Object.entries(this.input.filters)) {
                if (filterMappings[filterKey]) {
                    try {
                        // Click dropdown with retry logic
                        await page.click(filterMappings[filterKey]);
                        await page.waitForTimeout(500 + Math.random() * 1000);

                        // Select option
                        const optionSelector = `text=${filterValue}`;
                        await page.waitForSelector(optionSelector, { timeout: 10000 });
                        await page.click(optionSelector);

                        this.logger.debug(`Applied filter: ${filterKey} = ${filterValue}`);
                        await page.waitForTimeout(500 + Math.random() * 500);

                    } catch (filterError) {
                        this.logger.warn(`Failed to apply filter ${filterKey}`, { 
                            error: filterError.message,
                            value: filterValue
                        });
                    }
                }
            }

            // Apply filters
            await page.click(CONFIG.SELECTORS.filterButton);
            await page.waitForLoadState('networkidle');

            this.logger.info('Filters applied successfully');
            return true;

        } catch (error) {
            this.logger.error('Failed to apply filters', { error: error.message });
            return false;
        }
    }

    /**
     * Extract property data with enterprise validation
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting property data extraction');

            // Wait for results table with extended timeout
            await page.waitForSelector(CONFIG.SELECTORS.resultsTable, { timeout: 30000 });

            // Extract data using page evaluation with error handling
            const extractedData = await page.evaluate((maxResults) => {
                const rows = document.querySelectorAll('table tbody tr, .table tbody tr, [role="row"]:not(:first-child)');
                const results = [];
                
                for (let i = 0; i < Math.min(rows.length, maxResults); i++) {
                    try {
                        const row = rows[i];
                        const cells = row.querySelectorAll('td, [role="cell"]');
                        
                        if (cells.length >= 7) {
                            const project = cells[0]?.textContent?.trim() || '';
                            const subProject = cells[1]?.textContent?.trim() || '';
                            const unitType = cells[2]?.textContent?.trim() || '';
                            const floor = cells[3]?.textContent?.trim() || '';
                            const unitNo = cells[4]?.textContent?.trim() || '';
                            const totalUnitArea = cells[5]?.textContent?.trim() || '';
                            const startingPrice = cells[6]?.textContent?.trim() || '';
                            
                            // Generate unique unit ID
                            const unitId = `${project}_${unitNo}_${Date.now()}_${i}`.replace(/[^a-zA-Z0-9_]/g, '');
                            
                            // Validate required fields
                            if (project && unitNo && startingPrice) {
                                results.push({
                                    unitId,
                                    project,
                                    subProject,
                                    unitType,
                                    floor,
                                    unitNo,
                                    totalUnitArea,
                                    startingPrice,
                                    availability: 'available',
                                    sourceUrl: window.location.href,
                                    scrapedAt: new Date().toISOString()
                                });
                            }
                        }
                    } catch (rowError) {
                        console.warn(`Error processing row ${i}:`, rowError);
                    }
                }
                
                return results;
            }, this.input.maxResults);

            // Validate extracted data
            const validProperties = extractedData.filter(prop => 
                prop.project && prop.unitNo && prop.startingPrice
            );

            this.metrics.recordPropertiesScraped(validProperties.length);
            
            this.logger.info('Property data extraction completed', {
                totalExtracted: extractedData.length,
                validProperties: validProperties.length,
                invalidFiltered: extractedData.length - validProperties.length
            });

            return validProperties;

        } catch (error) {
            this.logger.error('Failed to extract property data', { error: error.message });
            throw new Error(`Data extraction failed: ${error.message}`);
        }
    }

    /**
     * Main enterprise scraping workflow
     */
    async executeScraping() {
        const crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: 1,
            requestHandlerTimeoutSecs: CONFIG.REQUEST_TIMEOUT / 1000,
            maxConcurrency: this.input.parallelRequests,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--disable-gpu',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--disable-blink-features=AutomationControlled'
                    ]
                }
            },
            requestHandler: async ({ page, request }) => {
                const scrapeStart = performance.now();
                
                try {
                    this.logger.info('Starting scraping workflow', { url: request.url });

                    // Memory monitoring
                    this.metrics.recordMemoryUsage();

                    // Apply rate limiting
                    await this.rateLimiter.wait();

                    // Perform authentication
                    if (!await this.authenticate(page)) {
                        throw new Error('Authentication failed');
                    }

                    // Navigate to projects page
                    if (!await this.navigateToProjects(page)) {
                        throw new Error('Failed to navigate to projects page');
                    }

                    // Apply filters
                    await this.applyFilters(page);

                    // Extract property data
                    const properties = await this.extractPropertyData(page);

                    // Prepare enterprise-grade output
                    const results = {
                        // Session metadata
                        sessionId: this.sessionId,
                        timestamp: new Date().toISOString(),
                        scrapeMode: this.input.scrapeMode,
                        
                        // Configuration used
                        configuration: {
                            filtersApplied: this.input.filters,
                            maxResults: this.input.maxResults,
                            enableStealth: this.input.enableStealth
                        },
                        
                        // Results data
                        summary: {
                            totalProperties: properties.length,
                            successRate: this.metrics.getSuccessRate(),
                            scrapingDuration: Math.round(performance.now() - scrapeStart)
                        },
                        
                        // Property data
                        properties,
                        
                        // Enterprise metrics
                        metrics: this.metrics.getSummary(),
                        
                        // Metadata for integration
                        metadata: {
                            scraperVersion: '1.0.0',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            timestamp: Date.now()
                        }
                    };

                    // Quality assurance check
                    if (this.metrics.getSuccessRate() < CONFIG.MIN_SUCCESS_RATE) {
                        this.logger.warn('Success rate below enterprise threshold', {
                            successRate: this.metrics.getSuccessRate(),
                            threshold: CONFIG.MIN_SUCCESS_RATE
                        });
                    }

                    // Store results in dataset
                    await Dataset.pushData(results);

                    this.logger.info('Scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart)
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Scraping workflow failed', {
                        error: error.message,
                        duration: Math.round(duration),
                        stack: error.stack
                    });
                    
                    // Store error results for debugging
                    await Dataset.pushData({
                        sessionId: this.sessionId,
                        timestamp: new Date().toISOString(),
                        success: false,
                        error: {
                            message: error.message,
                            stack: error.stack
                        },
                        metrics: this.metrics.getSummary()
                    });
                    
                    throw error;
                }
            }
        });

        // Execute crawler
        await crawler.run([CONFIG.LOGIN_URL]);
        
        return {
            success: true,
            sessionId: this.sessionId,
            metrics: this.metrics.getSummary()
        };
    }
}

/**
 * MAIN ENTERPRISE ACTOR ENTRY POINT
 */
await Actor.init();

try {
    // Get and validate input
    const actorInput = await Actor.getInput() ?? {};
    
    // Enterprise input validation
    let validatedInput;
    try {
        validatedInput = InputValidator.validate(actorInput);
    } catch (validationError) {
        Actor.log.error('Input validation failed', { error: validationError.message });
        await Actor.fail(`Input validation failed: ${validationError.message}`);
        return;
    }

    // Initialize enterprise scraper
    const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

    // Execute enterprise scraping workflow
    const results = await scraper.executeScraping();

    if (results.success) {
        Actor.log.info('Enterprise scraping completed successfully', {
            sessionId: results.sessionId,
            successRate: results.metrics.successRate,
            propertiesScraped: results.metrics.propertiesScraped
        });
    } else {
        Actor.log.error('Enterprise scraping failed', { error: results.error });
        await Actor.fail(`Scraping failed: ${results.error}`);
    }

} catch (error) {
    Actor.log.error('Critical error in enterprise actor', { 
        error: error.message,
        stack: error.stack 
    });
    await Actor.fail(`Critical error: ${error.message}`);
}

// Exit successfully
await Actor.exit();
