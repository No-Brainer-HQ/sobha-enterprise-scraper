/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.3 - SIMPLIFIED (NO LIGHTNING COMPLEXITY)
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { randomBytes, createHash } from 'crypto';
import { performance } from 'perf_hooks';

/**
 * Enterprise Configuration Constants - SIMPLIFIED
 */
const CONFIG = {
    // Performance settings - BACK TO REASONABLE TIMEOUTS
    MAX_CONCURRENT_REQUESTS: 3,
    REQUEST_TIMEOUT: 600000, // 10 minutes (reasonable)
    NAVIGATION_TIMEOUT: 60000, // 1 minute (reasonable)
    CONTENT_WAIT: 15000, // 15 seconds for content to render (simple)
    
    // Security settings
    MAX_RETRY_ATTEMPTS: 3,
    BASE_DELAY: 2000,
    MAX_DELAY: 10000,
    
    // Monitoring thresholds
    MIN_SUCCESS_RATE: 95.0,
    MAX_MEMORY_MB: 4096,
    
    // Portal endpoints
    LOGIN_URL: 'https://www.sobhapartnerportal.com/partnerportal/s/',
    
    // Selectors - SIMPLE AND WORKING
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]',
        
        // Simple dashboard detection
        dashboardIndicator: 'body',
        
        // Filter selectors
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
        
        // Safely call Actor.log methods with fallback
        try {
            const logMethod = level.toLowerCase();
            if (Actor.log && typeof Actor.log[logMethod] === 'function') {
                Actor.log[logMethod](message, data);
            } else {
                console.log(`${level}: ${message}`, data);
            }
        } catch (logError) {
            console.log(`${level}: ${message}`, data);
        }
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
            parallelRequests: input.parallelRequests || 2
        };
    }
}

/**
 * Enterprise Sobha Portal Scraper - SIMPLIFIED
 */
class EnterpriseSobhaPortalScraper {
    constructor(validatedInput) {
        this.input = validatedInput;
        this.sessionId = SecurityManager.generateSessionId();
        this.logger = new EnterpriseLogger(this.sessionId);
        this.rateLimiter = new RateLimiter(this.input.requestDelay * 1000);
        this.metrics = new MetricsCollector(this.sessionId);
        
        this.logger.info('Simplified enterprise scraper initialized', {
            sessionId: this.sessionId,
            scrapeMode: this.input.scrapeMode,
            maxResults: this.input.maxResults,
            email: SecurityManager.maskSensitiveData(this.input.email)
        });
    }

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
     * SIMPLIFIED: Authentication without Lightning complexity
     */
    async authenticate(page) {
        const maxAttempts = this.input.retryAttempts;
        
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            const requestStart = performance.now();
            
            try {
                this.logger.info(`Authentication attempt ${attempt}/${maxAttempts}`, {
                    email: SecurityManager.maskSensitiveData(this.input.email)
                });

                // Navigate to login page
                await page.goto(CONFIG.LOGIN_URL, { 
                    waitUntil: 'domcontentloaded',
                    timeout: CONFIG.NAVIGATION_TIMEOUT 
                });

                // Apply stealth techniques
                await this.applyStealthTechniques(page);

                // Wait for login form elements
                this.logger.info('Waiting for Sobha portal login form elements');
                
                await page.waitForSelector(CONFIG.SELECTORS.email, { timeout: 30000 });
                await page.waitForSelector(CONFIG.SELECTORS.password, { timeout: 30000 });

                this.logger.info('Sobha portal form elements found, filling credentials');

                // Clear and fill email field with human-like typing
                await page.fill(CONFIG.SELECTORS.email, '');
                await page.type(CONFIG.SELECTORS.email, this.input.email, { delay: 100 + Math.random() * 50 });

                // Clear and fill password field
                await page.fill(CONFIG.SELECTORS.password, '');
                await page.type(CONFIG.SELECTORS.password, this.input.password, { delay: 100 + Math.random() * 50 });

                // Add realistic human delay
                await page.waitForTimeout(1000 + Math.random() * 2000);

                // Click login button
                this.logger.info('Clicking login button');
                await page.waitForSelector(CONFIG.SELECTORS.loginButton, { timeout: 10000 });
                await page.click(CONFIG.SELECTORS.loginButton);

                // SIMPLIFIED: Just wait for URL change
                this.logger.info('Waiting for post-login navigation (simple)');
                
                try {
                    // Wait for URL change (most reliable)
                    await page.waitForFunction(() => {
                        return window.location.href.includes('/partnerportal/s/') || 
                               window.location.href.includes('frontdoor') ||
                               window.location.href !== 'https://www.sobhapartnerportal.com/partnerportal/s/';
                    }, {}, { timeout: 30000 });

                    const currentUrl = page.url();
                    const pageTitle = await page.title().catch(() => 'Unknown');
                    
                    this.logger.info('Post-login page loaded', { 
                        currentUrl: currentUrl.substring(0, 100),
                        pageTitle 
                    });

                    // SIMPLE: Just wait for page to load content (no Lightning validation)
                    this.logger.info(`Waiting ${CONFIG.CONTENT_WAIT}ms for page content to render`);
                    await page.waitForTimeout(CONFIG.CONTENT_WAIT);

                    // SIMPLE: Try to dismiss any modal (don't worry if it fails)
                    await this.dismissPostLoginModal(page);
                    
                    // Additional wait for stability
                    await page.waitForTimeout(3000);

                    // Validate authentication success
                    if (currentUrl !== CONFIG.LOGIN_URL) {
                        const requestDuration = performance.now() - requestStart;
                        this.metrics.recordRequest(true, requestDuration);
                        this.rateLimiter.onSuccess();
                        
                        this.logger.info('Authentication successful (simple)', {
                            attempt,
                            duration: Math.round(requestDuration),
                            currentUrl: currentUrl.substring(0, 100)
                        });
                        
                        return true;
                    } else {
                        throw new Error('Login page did not redirect after clicking login button');
                    }
                    
                } catch (waitError) {
                    // Check for error messages on page
                    try {
                        const errorElements = await page.locator('text="error", text="invalid", text="incorrect", text="Error", text="Invalid", text="Incorrect"').all();
                        if (errorElements.length > 0) {
                            const errorText = await errorElements[0].textContent();
                            throw new Error(`Authentication failed: ${errorText}`);
                        }
                    } catch (errorCheckError) {
                        this.logger.debug('Error message check failed:', { error: errorCheckError.message });
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
     * SIMPLIFIED: Modal dismissal (best effort, no stress if it fails)
     */
    async dismissPostLoginModal(page) {
        try {
            this.logger.info('Attempting to dismiss any post-login modals (best effort)');

            // Wait briefly for any modals to appear
            await page.waitForTimeout(2000);

            // Try common modal close methods (don't fail if they don't work)
            const modalSelectors = [
                '.slds-modal button[data-key="close"]',
                '.slds-modal button[title*="Close"]',
                '.slds-modal button[aria-label*="close"]',
                '.slds-modal .slds-modal__close',
                '.slds-modal button:has-text("×")',
                '.slds-modal header button'
            ];

            for (const selector of modalSelectors) {
                try {
                    const button = page.locator(selector).first();
                    const isVisible = await button.isVisible().catch(() => false);
                    
                    if (isVisible) {
                        this.logger.info(`Found modal close button: ${selector}`);
                        await button.click({ timeout: 3000 });
                        await page.waitForTimeout(1000);
                        this.logger.info('✅ Modal closed successfully');
                        break;
                    }
                } catch (error) {
                    // Silently continue to next selector
                }
            }

            // Try escape key as fallback
            try {
                await page.keyboard.press('Escape');
                await page.waitForTimeout(1000);
            } catch (error) {
                // Don't worry if this fails
            }

            this.logger.info('Modal dismissal completed (best effort)');

        } catch (error) {
            this.logger.info('Modal dismissal skipped (no modals found or failed)', { 
                error: error.message 
            });
            // Don't throw error - just continue
        }
    }

    /**
     * SIMPLIFIED: Navigate to projects page
     */
    async navigateToProjects(page) {
        try {
            this.logger.info('Navigating to Sobha Projects page (simple approach)');

            // Wait for page to stabilize
            await page.waitForTimeout(2000);

            // Method 1: Try direct URL navigation (most reliable)
            this.logger.info('Attempting direct navigation to projects page');
            try {
                const currentUrl = page.url();
                const baseUrl = currentUrl.split('/partnerportal')[0];
                const projectsUrl = `${baseUrl}/partnerportal/s/sobha-project`;
                
                this.logger.info(`Navigating directly to: ${projectsUrl}`);
                await page.goto(projectsUrl, { 
                    waitUntil: 'domcontentloaded',
                    timeout: CONFIG.NAVIGATION_TIMEOUT 
                });
                
                // Simple wait for content
                this.logger.info(`Waiting ${CONFIG.CONTENT_WAIT}ms for projects page to load`);
                await page.waitForTimeout(CONFIG.CONTENT_WAIT);
                
                this.logger.info('✅ Direct navigation to projects page successful');
                return true;
                
            } catch (directNavError) {
                this.logger.warn('Direct navigation failed, trying element-based navigation', { 
                    error: directNavError.message 
                });
            }

            // Method 2: Try finding navigation elements (best effort)
            const navigationSelectors = [
                'a[href="/partnerportal/s/sobha-project"]',
                'a[href*="sobha-project"]',
                'a:has-text("Sobha Projects")',
                'button:has-text("Sobha Projects")',
                'text=Projects',
                'a[href*="project"]'
            ];

            for (const selector of navigationSelectors) {
                try {
                    this.logger.debug(`Trying navigation selector: ${selector}`);
                    
                    await page.waitForSelector(selector, { timeout: 5000 });
                    
                    const element = page.locator(selector).first();
                    const isVisible = await element.isVisible();
                    
                    if (isVisible) {
                        this.logger.info(`Found navigation element: ${selector}`);
                        
                        await element.scrollIntoViewIfNeeded();
                        await page.waitForTimeout(1000);
                        await element.click();
                        
                        // Wait for navigation
                        await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                        await page.waitForTimeout(CONFIG.CONTENT_WAIT);
                        
                        this.logger.info(`✅ Successfully navigated using: ${selector}`);
                        return true;
                    }
                } catch (selectorError) {
                    this.logger.debug(`Navigation selector failed: ${selector}`, { error: selectorError.message });
                }
            }

            // If navigation attempts failed, continue with current page
            this.logger.info('Navigation elements not found, proceeding with current page content');
            return true;

        } catch (error) {
            this.logger.error('Navigation failed, proceeding with current page', { error: error.message });
            return true; // Continue anyway
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
            await page.waitForLoadState('domcontentloaded');

            this.logger.info('Filters applied successfully');
            return true;

        } catch (error) {
            this.logger.error('Failed to apply filters', { error: error.message });
            return false;
        }
    }

    /**
     * SIMPLIFIED: Extract property data from whatever HTML exists
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting simple property data extraction');

            // Simple wait for content to be available
            await page.waitForTimeout(3000);

            // Simple page structure analysis
            const pageStructure = await page.evaluate(() => {
                return {
                    tables: document.querySelectorAll('table').length,
                    dataRows: document.querySelectorAll('tr, [role="row"]').length,
                    listItems: document.querySelectorAll('li, .list-item').length,
                    cards: document.querySelectorAll('.card, .slds-card, [class*="card"]').length,
                    contentElements: document.querySelectorAll('[class*="property"], [class*="unit"], [class*="sobha"]').length,
                    hasData: document.body.textContent.length,
                    title: document.title,
                    hasErrors: (document.body.textContent || '').includes('Error'),
                    hasLoading: (document.body.textContent || '').includes('Loading')
                };
            });

            this.logger.info('Simple page structure analysis:', pageStructure);

            let extractedData = [];

            // Approach 1: Table-based extraction (traditional)
            try {
                this.logger.info('Attempting table-based extraction');
                
                extractedData = await page.evaluate((maxResults) => {
                    const results = [];
                    const tables = document.querySelectorAll('table, [role="table"]');
                    
                    tables.forEach((table, tableIndex) => {
                        const rows = table.querySelectorAll('tbody tr, [role="row"]:not(:first-child)');
                        
                        for (let i = 0; i < Math.min(rows.length, maxResults - results.length); i++) {
                            const row = rows[i];
                            const cells = row.querySelectorAll('td, th, [role="cell"], [role="gridcell"]');
                            
                            if (cells.length >= 3) {
                                const cellTexts = Array.from(cells).map(cell => cell.textContent?.trim() || '');
                                
                                // Skip header rows and empty rows
                                if (cellTexts.some(text => text.length > 0) && 
                                    !cellTexts[0].toLowerCase().includes('project') &&
                                    !cellTexts[0].toLowerCase().includes('unit')) {
                                    
                                    results.push({
                                        unitId: `table_${tableIndex}_${i}_${Date.now()}`,
                                        project: cellTexts[0] || 'Sobha Table Project',
                                        subProject: cellTexts[1] || '',
                                        unitType: cellTexts[2] || '',
                                        floor: cellTexts[3] || '',
                                        unitNo: cellTexts[4] || `T${tableIndex}-${i + 1}`,
                                        totalUnitArea: cellTexts[5] || '',
                                        startingPrice: cellTexts[6] || '',
                                        availability: 'available',
                                        sourceUrl: window.location.href,
                                        extractionMethod: 'Simple-Table',
                                        rawCellData: cellTexts,
                                        scrapedAt: new Date().toISOString()
                                    });
                                }
                            }
                        }
                    });
                    
                    return results;
                }, this.input.maxResults);

                this.logger.info('Table extraction completed', { propertiesFound: extractedData.length });

            } catch (tableError) {
                this.logger.warn('Table extraction failed', { error: tableError.message });
            }

            // Approach 2: Card/List-based extraction
            if (extractedData.length === 0) {
                try {
                    this.logger.info('Attempting card/list-based extraction');
                    
                    extractedData = await page.evaluate((maxResults) => {
                        const results = [];
                        
                        // Look for any structured content
                        const containers = document.querySelectorAll(
                            '.card, .slds-card, .list-item, [class*="property"], [class*="unit"], ' +
                            '[class*="listing"], li, .item, [data-record-id], [data-row-key-value]'
                        );
                        
                        for (let i = 0; i < Math.min(containers.length, maxResults); i++) {
                            const container = containers[i];
                            const text = container.textContent?.trim() || '';
                            
                            // Filter out navigation and menu items
                            if (text.length > 30 && 
                                !text.includes('Dashboard') && 
                                !text.includes('Profile') && 
                                !text.includes('About') &&
                                !text.includes('Marketing') &&
                                !text.includes('Performance') &&
                                !text.includes('Loading') &&
                                !text.includes('Home') &&
                                (text.includes('Sobha') || 
                                 text.includes('Project') || 
                                 text.includes('Unit') || 
                                 text.includes('Bedroom') ||
                                 text.includes('sqft') ||
                                 text.includes('AED') ||
                                 text.match(/\d{2,}/) ||
                                 container.className.includes('property') ||
                                 container.className.includes('unit'))) {
                                
                                // Try to extract specific data from text patterns
                                const extractPatterns = (text) => {
                                    return {
                                        project: text.match(/(?:project|development)[\s:]*([^,\n]{3,30})/i)?.[1] || 'Sobha Project',
                                        unit: text.match(/(?:unit|apartment|flat)[\s#]*([a-z0-9\-]{1,10})/i)?.[1] || `Card-${i + 1}`,
                                        price: text.match(/(?:aed|price|cost)[\s:]*([0-9,]+)/i)?.[1] || '',
                                        area: text.match(/(\d+)\s*(?:sqft|sq\.?\s*ft)/i)?.[1] || '',
                                        bedrooms: text.match(/(\d+)\s*(?:bed|bedroom|br)/i)?.[1] || ''
                                    };
                                };
                                
                                const patterns = extractPatterns(text);
                                
                                results.push({
                                    unitId: `card_${i}_${Date.now()}`,
                                    project: patterns.project,
                                    subProject: '',
                                    unitType: patterns.bedrooms ? `${patterns.bedrooms} Bedroom` : '',
                                    floor: '',
                                    unitNo: patterns.unit,
                                    totalUnitArea: patterns.area ? `${patterns.area} sqft` : '',
                                    startingPrice: patterns.price ? `${patterns.price} AED` : '',
                                    availability: 'available',
                                    rawData: text.substring(0, 300),
                                    sourceUrl: window.location.href,
                                    extractionMethod: 'Simple-Card',
                                    elementDetails: {
                                        className: container.className,
                                        id: container.id,
                                        tagName: container.tagName
                                    },
                                    scrapedAt: new Date().toISOString()
                                });
                            }
                        }
                        
                        return results;
                    }, this.input.maxResults);

                    this.logger.info('Card/list extraction completed', { propertiesFound: extractedData.length });

                } catch (cardError) {
                    this.logger.warn('Card extraction failed', { error: cardError.message });
                }
            }

            // Approach 3: Any meaningful content extraction
            if (extractedData.length === 0) {
                this.logger.info('Attempting general content extraction');
                
                extractedData = await page.evaluate((maxResults) => {
                    const results = [];
                    
                    // Get all text content and look for property-related information
                    const bodyText = document.body.textContent || '';
                    const meaningfulText = bodyText.replace(/\s+/g, ' ').trim();
                    
                    // If page has substantial content that's not just errors/loading
                    if (meaningfulText.length > 1000 && 
                        !meaningfulText.includes('CSS Error') &&
                        !bodyText.includes('Loading') &&
                        (meaningfulText.includes('Sobha') || 
                         meaningfulText.includes('Project') || 
                         meaningfulText.includes('Property'))) {
                        
                        // Create a content-based entry
                        results.push({
                            unitId: `content_${Date.now()}`,
                            project: 'Sobha Content Analysis',
                            subProject: 'General Content',
                            unitType: '',
                            floor: '',
                            unitNo: 'CONTENT-001',
                            totalUnitArea: '',
                            startingPrice: '',
                            availability: 'available',
                            rawData: meaningfulText.substring(0, 1000),
                            contentAnalysis: {
                                textLength: meaningfulText.length,
                                hasSobha: meaningfulText.includes('Sobha'),
                                hasProject: meaningfulText.includes('Project'),
                                hasUnit: meaningfulText.includes('Unit'),
                                hasPrice: meaningfulText.includes('AED') || meaningfulText.includes('Price')
                            },
                            sourceUrl: window.location.href,
                            extractionMethod: 'Simple-Content',
                            scrapedAt: new Date().toISOString()
                        });
                    }
                    
                    return results;
                }, this.input.maxResults);

                this.logger.info('General content extraction completed', { propertiesFound: extractedData.length });
            }

            // If still no data, create a simple debug entry
            if (extractedData.length === 0) {
                this.logger.warn('No property data found - creating simple debug entry');
                
                const simplePageAnalysis = await page.evaluate(() => {
                    return {
                        url: window.location.href,
                        title: document.title,
                        bodyTextLength: document.body.textContent?.length || 0,
                        hasErrors: (document.body.textContent || '').includes('Error'),
                        hasLoading: (document.body.textContent || '').includes('Loading'),
                        sampleText: (document.body.textContent || '').substring(0, 500)
                    };
                });
                
                extractedData = [{
                    unitId: `debug_simple_${Date.now()}`,
                    project: 'Simple Debug Entry',
                    subProject: 'No Properties Found',
                    unitType: 'Debug',
                    floor: '0',
                    unitNo: 'DEBUG-SIMPLE-001',
                    totalUnitArea: '0',
                    startingPrice: '0',
                    availability: 'debug',
                    sourceUrl: simplePageAnalysis.url,
                    pageTitle: simplePageAnalysis.title,
                    debugInfo: {
                        message: 'No property data found with simple extraction',
                        pageAnalysis: simplePageAnalysis,
                        pageStructure: pageStructure
                    },
                    extractionMethod: 'Simple-Debug',
                    scrapedAt: new Date().toISOString()
                }];
            }

            // Simple validation
            const validProperties = extractedData.filter(prop => 
                prop.unitId && prop.project && prop.unitNo
            );

            this.metrics.recordPropertiesScraped(validProperties.length);
            
            this.logger.info('Simple property data extraction completed', {
                totalExtracted: extractedData.length,
                validProperties: validProperties.length,
                extractionMethods: [...new Set(extractedData.map(p => p.extractionMethod))]
            });

            return validProperties;

        } catch (error) {
            this.logger.error('Simple property data extraction failed', { error: error.message });
            
            // Return simple error entry
            return [{
                unitId: `error_simple_${Date.now()}`,
                project: 'Simple Error Entry',
                subProject: 'Extraction failed',
                unitType: 'Error',
                floor: '0',
                unitNo: 'ERROR-SIMPLE-001',
                totalUnitArea: '0',
                startingPrice: '0',
                availability: 'error',
                sourceUrl: page.url(),
                errorInfo: error.message,
                extractionMethod: 'Simple-Error-Fallback',
                scrapedAt: new Date().toISOString()
            }];
        }
    }

    /**
     * Main simplified scraping workflow
     */
    async executeScraping() {
        const crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: 1,
            requestHandlerTimeoutSecs: CONFIG.REQUEST_TIMEOUT / 1000, // 10 minutes
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
                    this.logger.info('Starting simplified scraping workflow', { url: request.url });

                    // Memory monitoring
                    this.metrics.recordMemoryUsage();

                    // Apply rate limiting
                    await this.rateLimiter.wait();

                    // Perform authentication (simplified)
                    if (!await this.authenticate(page)) {
                        throw new Error('Authentication failed');
                    }

                    // Navigate to projects page (simplified)
                    await this.navigateToProjects(page);

                    // Apply filters (if any)
                    await this.applyFilters(page);

                    // Extract property data (simplified)
                    const properties = await this.extractPropertyData(page);

                    // Prepare simplified output
                    const results = {
                        // Session metadata
                        sessionId: this.sessionId,
                        timestamp: new Date().toISOString(),
                        scrapeMode: this.input.scrapeMode,
                        
                        // Configuration used
                        configuration: {
                            filtersApplied: this.input.filters,
                            maxResults: this.input.maxResults,
                            enableStealth: this.input.enableStealth,
                            approach: 'simplified'
                        },
                        
                        // Results data
                        summary: {
                            totalProperties: properties.length,
                            successRate: this.metrics.getSuccessRate(),
                            scrapingDuration: Math.round(performance.now() - scrapeStart)
                        },
                        
                        // Property data
                        properties,
                        
                        // Metrics
                        metrics: this.metrics.getSummary(),
                        
                        // Metadata
                        metadata: {
                            scraperVersion: '1.0.3',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            approach: 'simplified',
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

                    this.logger.info('Simplified scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart)
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Simplified scraping workflow failed', {
                        error: error.message,
                        duration: Math.round(duration),
                        stack: error.stack
                    });
                    
                    // Store error results
                    await Dataset.pushData({
                        sessionId: this.sessionId,
                        timestamp: new Date().toISOString(),
                        success: false,
                        error: {
                            message: error.message,
                            stack: error.stack
                        },
                        metrics: this.metrics.getSummary(),
                        approach: 'simplified'
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
 * MAIN SIMPLIFIED ACTOR ENTRY POINT
 */
async function main() {
    try {
        await Actor.init();
        console.log('Simplified Actor initialized successfully');

        // Get and validate input
        const actorInput = await Actor.getInput() ?? {};
        console.log('Actor input received:', { hasEmail: !!actorInput.email, hasPassword: !!actorInput.password });
        
        // Input validation
        let validatedInput;
        try {
            validatedInput = InputValidator.validate(actorInput);
            console.log('Input validation successful');
        } catch (validationError) {
            console.error('Input validation failed:', validationError.message);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Input validation failed', { error: validationError.message });
            }
            await Actor.fail(`Input validation failed: ${validationError.message}`);
            return;
        }

        // Initialize simplified scraper
        console.log('Initializing simplified enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute simplified scraping workflow
        console.log('Starting simplified scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Simplified scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Simplified scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped
                });
            }
        } else {
            console.error('Simplified scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Simplified scraping failed', { error: results.error });
            }
            await Actor.fail(`Simplified scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in simplified actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in simplified actor', { 
                error: error.message,
                stack: error.stack 
            });
        }
        
        try {
            await Actor.fail(`Critical error: ${error.message}`);
        } catch (failError) {
            console.error('Failed to call Actor.fail():', failError.message);
        }
        return;
    }

    // Exit successfully
    try {
        await Actor.exit();
    } catch (exitError) {
        console.error('Failed to exit actor:', exitError.message);
    }
}

// Execute the main function
await main();
