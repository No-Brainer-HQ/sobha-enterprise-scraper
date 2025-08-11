/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.4 - FIXED PROPERTY MODAL EXTRACTION
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { randomBytes, createHash } from 'crypto';
import { performance } from 'perf_hooks';

/**
 * Enterprise Configuration Constants - OPTIMIZED FOR MODAL EXTRACTION
 */
const CONFIG = {
    // Performance settings
    MAX_CONCURRENT_REQUESTS: 2,
    REQUEST_TIMEOUT: 600000, // 10 minutes
    NAVIGATION_TIMEOUT: 60000, // 1 minute
    CONTENT_WAIT: 10000, // 10 seconds for content to render
    MODAL_WAIT: 15000, // 15 seconds for modal to load
    
    // Security settings
    MAX_RETRY_ATTEMPTS: 3,
    BASE_DELAY: 2000,
    MAX_DELAY: 10000,
    
    // Monitoring thresholds
    MIN_SUCCESS_RATE: 95.0,
    MAX_MEMORY_MB: 4096,
    
    // Portal endpoints
    LOGIN_URL: 'https://www.sobhapartnerportal.com/partnerportal/s/',
    
    // Selectors - UPDATED FOR MODAL EXTRACTION
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]',
        
        // Modal-specific selectors
        filterPropertiesButton: 'button:has-text("Filter Properties")',
        propertyModal: '[role="dialog"]:has-text("Available Units"), .slds-modal:has-text("Available Units")',
        modalTable: 'table, [role="table"], [role="grid"]',
        modalTableRows: 'tbody tr, [role="row"]:not(:first-child)',
        modalTableCells: 'td, [role="gridcell"], [role="cell"]',
        
        // Simple dashboard detection
        dashboardIndicator: 'body'
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
 * Enterprise Sobha Portal Scraper - WITH MODAL EXTRACTION
 */
class EnterpriseSobhaPortalScraper {
    constructor(validatedInput) {
        this.input = validatedInput;
        this.sessionId = SecurityManager.generateSessionId();
        this.logger = new EnterpriseLogger(this.sessionId);
        this.rateLimiter = new RateLimiter(this.input.requestDelay * 1000);
        this.metrics = new MetricsCollector(this.sessionId);
        
        this.logger.info('Modal-aware enterprise scraper initialized', {
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
     * Authentication (unchanged - working perfectly)
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

                // Wait for URL change
                this.logger.info('Waiting for post-login navigation');
                
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

                    // Simple wait for content
                    this.logger.info(`Waiting ${CONFIG.CONTENT_WAIT}ms for page content to render`);
                    await page.waitForTimeout(CONFIG.CONTENT_WAIT);

                    // Try to dismiss any modal (best effort)
                    await this.dismissPostLoginModal(page);
                    
                    // Additional wait for stability
                    await page.waitForTimeout(3000);

                    // Validate authentication success
                    if (currentUrl !== CONFIG.LOGIN_URL) {
                        const requestDuration = performance.now() - requestStart;
                        this.metrics.recordRequest(true, requestDuration);
                        this.rateLimiter.onSuccess();
                        
                        this.logger.info('Authentication successful', {
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
     * Modal dismissal (best effort)
     */
    async dismissPostLoginModal(page) {
        try {
            this.logger.info('Attempting to dismiss any post-login modals (best effort)');

            // Wait briefly for any modals to appear
            await page.waitForTimeout(2000);

            // Try common modal close methods
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
            this.logger.info('Modal dismissal skipped', { error: error.message });
        }
    }

    /**
     * Navigate to projects page and open property modal
     */
    async navigateToProjects(page) {
        try {
            this.logger.info('Navigating to Sobha Projects page');

            // Wait for page to stabilize
            await page.waitForTimeout(2000);

            // Navigate directly to projects page
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
                
                // Wait for page content to load
                this.logger.info(`Waiting ${CONFIG.CONTENT_WAIT}ms for projects page to load`);
                await page.waitForTimeout(CONFIG.CONTENT_WAIT);
                
                this.logger.info('✅ Direct navigation to projects page successful');
                return true;
                
            } catch (directNavError) {
                this.logger.warn('Direct navigation failed', { error: directNavError.message });
                throw directNavError;
            }

        } catch (error) {
            this.logger.error('Failed to navigate to projects page', { error: error.message });
            throw error;
        }
    }

    /**
     * CRITICAL NEW METHOD: Open property modal by clicking Filter Properties
     */
    async openPropertyModal(page) {
        try {
            this.logger.info('Opening property listings modal');

            // Wait for page to be ready
            await page.waitForTimeout(3000);

            // Find and click the "Filter Properties" button
            this.logger.info('Looking for "Filter Properties" button');
            
            try {
                // Wait for the button to be present and visible
                await page.waitForSelector(CONFIG.SELECTORS.filterPropertiesButton, { 
                    timeout: 15000,
                    state: 'visible'
                });

                this.logger.info('Found "Filter Properties" button, clicking it');
                
                // Click the button
                await page.click(CONFIG.SELECTORS.filterPropertiesButton);
                
                // Wait for modal to appear
                this.logger.info('Waiting for property modal to load');
                await page.waitForSelector(CONFIG.SELECTORS.propertyModal, { 
                    timeout: CONFIG.MODAL_WAIT,
                    state: 'visible'
                });

                // Additional wait for modal content to load
                await page.waitForTimeout(3000);

                this.logger.info('✅ Property modal opened successfully');
                return true;

            } catch (buttonError) {
                this.logger.error('Failed to find or click Filter Properties button', { 
                    error: buttonError.message 
                });
                
                // Try alternative selectors for the button
                const alternativeButtonSelectors = [
                    'button:has-text("Filter Properties")',
                    'input[value*="Filter Properties"]',
                    '[onclick*="filter"], [onclick*="Filter"]',
                    'button:contains("Filter Properties")',
                    '.filter-button, .filterButton',
                    'button[type="submit"]'
                ];

                for (const selector of alternativeButtonSelectors) {
                    try {
                        this.logger.debug(`Trying alternative button selector: ${selector}`);
                        await page.waitForSelector(selector, { timeout: 5000 });
                        await page.click(selector);
                        
                        // Check if modal appeared
                        await page.waitForSelector(CONFIG.SELECTORS.propertyModal, { 
                            timeout: 10000,
                            state: 'visible'
                        });
                        
                        this.logger.info(`✅ Modal opened with alternative selector: ${selector}`);
                        return true;
                        
                    } catch (altError) {
                        this.logger.debug(`Alternative selector failed: ${selector}`, { error: altError.message });
                    }
                }
                
                throw new Error('Could not find or click Filter Properties button');
            }

        } catch (error) {
            this.logger.error('Failed to open property modal', { error: error.message });
            throw error;
        }
    }

    /**
     * ENHANCED: Extract property data from modal table
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting modal-based property data extraction');

            // Wait for modal content to be stable
            await page.waitForTimeout(2000);

            // Check if modal is open and has content
            const modalExists = await page.locator(CONFIG.SELECTORS.propertyModal).count();
            if (modalExists === 0) {
                throw new Error('Property modal is not open');
            }

            this.logger.info('Property modal is open, extracting table data');

            // Extract data from the modal table
            const extractedData = await page.evaluate((maxResults) => {
                const results = [];
                
                // Find the modal and table within it
                const modal = document.querySelector('[role="dialog"]:has-text("Available Units"), .slds-modal:has-text("Available Units")') ||
                             document.querySelector('[role="dialog"], .slds-modal');
                
                if (!modal) {
                    console.log('No modal found');
                    return results;
                }
                
                console.log('Modal found, looking for table');
                
                // Find table within the modal
                const tables = modal.querySelectorAll('table, [role="table"], [role="grid"]');
                console.log(`Found ${tables.length} tables in modal`);
                
                if (tables.length === 0) {
                    console.log('No tables found in modal');
                    return results;
                }
                
                // Extract data from the first table
                const table = tables[0];
                const rows = table.querySelectorAll('tbody tr, [role="row"]:not(:first-child)');
                console.log(`Found ${rows.length} data rows in table`);
                
                for (let i = 0; i < Math.min(rows.length, maxResults); i++) {
                    const row = rows[i];
                    const cells = row.querySelectorAll('td, [role="gridcell"], [role="cell"]');
                    
                    if (cells.length >= 6) { // Minimum expected columns
                        const cellTexts = Array.from(cells).map(cell => cell.textContent?.trim() || '');
                        
                        // Map cells to property data based on table structure
                        // Columns: Project, Sub Project, Unit Type, Floor, Unit No., Total Unit Area, Starting Price
                        const property = {
                            unitId: `sobha_${Date.now()}_${i}`,
                            project: cellTexts[0] || 'Unknown Project',
                            subProject: cellTexts[1] || '',
                            unitType: cellTexts[2] || '',
                            floor: cellTexts[3] || '',
                            unitNo: cellTexts[4] || `Unit-${i + 1}`,
                            totalUnitArea: cellTexts[5] || '',
                            startingPrice: cellTexts[6] || '',
                            availability: 'available',
                            sourceUrl: window.location.href,
                            extractionMethod: 'Modal-Table',
                            rawCellData: cellTexts,
                            scrapedAt: new Date().toISOString()
                        };
                        
                        // Only include if we have meaningful data
                        if (property.project !== 'Unknown Project' && property.unitNo && property.unitNo !== `Unit-${i + 1}`) {
                            results.push(property);
                            console.log(`Extracted property: ${property.project} - ${property.unitNo}`);
                        }
                    }
                }
                
                console.log(`Total properties extracted: ${results.length}`);
                return results;
                
            }, this.input.maxResults);

            this.logger.info('Modal table extraction completed', { propertiesFound: extractedData.length });

            // If we found properties, check if we need to scroll for more
            if (extractedData.length > 0) {
                try {
                    this.logger.info('Checking for additional properties by scrolling');
                    
                    // Scroll within the modal to load more properties
                    await page.evaluate(() => {
                        const modal = document.querySelector('[role="dialog"], .slds-modal');
                        if (modal) {
                            const scrollableArea = modal.querySelector('.slds-scrollable, .scroll, [style*="overflow"]') || modal;
                            scrollableArea.scrollTop = scrollableArea.scrollHeight;
                        }
                    });
                    
                    // Wait for potential new content
                    await page.waitForTimeout(3000);
                    
                    // Extract additional data after scrolling
                    const additionalData = await page.evaluate((maxResults, currentCount) => {
                        const results = [];
                        const modal = document.querySelector('[role="dialog"], .slds-modal');
                        
                        if (modal) {
                            const table = modal.querySelector('table, [role="table"], [role="grid"]');
                            if (table) {
                                const rows = table.querySelectorAll('tbody tr, [role="row"]:not(:first-child)');
                                
                                // Only process rows beyond what we already have
                                for (let i = currentCount; i < Math.min(rows.length, maxResults); i++) {
                                    const row = rows[i];
                                    const cells = row.querySelectorAll('td, [role="gridcell"], [role="cell"]');
                                    
                                    if (cells.length >= 6) {
                                        const cellTexts = Array.from(cells).map(cell => cell.textContent?.trim() || '');
                                        
                                        const property = {
                                            unitId: `sobha_scroll_${Date.now()}_${i}`,
                                            project: cellTexts[0] || 'Unknown Project',
                                            subProject: cellTexts[1] || '',
                                            unitType: cellTexts[2] || '',
                                            floor: cellTexts[3] || '',
                                            unitNo: cellTexts[4] || `Unit-${i + 1}`,
                                            totalUnitArea: cellTexts[5] || '',
                                            startingPrice: cellTexts[6] || '',
                                            availability: 'available',
                                            sourceUrl: window.location.href,
                                            extractionMethod: 'Modal-Table-Scroll',
                                            rawCellData: cellTexts,
                                            scrapedAt: new Date().toISOString()
                                        };
                                        
                                        if (property.project !== 'Unknown Project' && property.unitNo && property.unitNo !== `Unit-${i + 1}`) {
                                            results.push(property);
                                        }
                                    }
                                }
                            }
                        }
                        
                        return results;
                    }, this.input.maxResults, extractedData.length);
                    
                    // Combine original and additional data
                    extractedData.push(...additionalData);
                    
                    this.logger.info('Scroll extraction completed', { 
                        additionalProperties: additionalData.length,
                        totalProperties: extractedData.length 
                    });
                    
                } catch (scrollError) {
                    this.logger.warn('Failed to scroll for additional properties', { error: scrollError.message });
                }
            }

            // If no properties found, create debug entry
            if (extractedData.length === 0) {
                this.logger.warn('No property data found in modal - creating debug entry');
                
                const modalAnalysis = await page.evaluate(() => {
                    const modal = document.querySelector('[role="dialog"], .slds-modal');
                    if (!modal) return { error: 'No modal found' };
                    
                    return {
                        modalText: modal.textContent?.substring(0, 1000) || '',
                        tableCount: modal.querySelectorAll('table, [role="table"]').length,
                        rowCount: modal.querySelectorAll('tr, [role="row"]').length,
                        modalClasses: modal.className || '',
                        modalHTML: modal.innerHTML?.substring(0, 2000) || ''
                    };
                });
                
                extractedData.push({
                    unitId: `debug_modal_${Date.now()}`,
                    project: 'Modal Debug Entry',
                    subProject: 'No Properties Found',
                    unitType: 'Debug',
                    floor: '0',
                    unitNo: 'DEBUG-MODAL-001',
                    totalUnitArea: '0',
                    startingPrice: '0',
                    availability: 'debug',
                    sourceUrl: page.url(),
                    debugInfo: {
                        message: 'No property data found in modal',
                        modalAnalysis: modalAnalysis
                    },
                    extractionMethod: 'Modal-Debug',
                    scrapedAt: new Date().toISOString()
                });
            }

            // Validate extracted data
            const validProperties = extractedData.filter(prop => 
                prop.unitId && prop.project && prop.unitNo
            );

            this.metrics.recordPropertiesScraped(validProperties.length);
            
            this.logger.info('Modal property data extraction completed', {
                totalExtracted: extractedData.length,
                validProperties: validProperties.length,
                extractionMethods: [...new Set(extractedData.map(p => p.extractionMethod))]
            });

            return validProperties;

        } catch (error) {
            this.logger.error('Modal property data extraction failed', { error: error.message });
            
            // Return error entry
            return [{
                unitId: `error_modal_${Date.now()}`,
                project: 'Modal Error Entry',
                subProject: 'Extraction failed',
                unitType: 'Error',
                floor: '0',
                unitNo: 'ERROR-MODAL-001',
                totalUnitArea: '0',
                startingPrice: '0',
                availability: 'error',
                sourceUrl: page.url(),
                errorInfo: error.message,
                extractionMethod: 'Modal-Error-Fallback',
                scrapedAt: new Date().toISOString()
            }];
        }
    }

    /**
     * Main enhanced scraping workflow with modal extraction
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
                    this.logger.info('Starting modal-aware scraping workflow', { url: request.url });

                    // Memory monitoring
                    this.metrics.recordMemoryUsage();

                    // Apply rate limiting
                    await this.rateLimiter.wait();

                    // Perform authentication
                    if (!await this.authenticate(page)) {
                        throw new Error('Authentication failed');
                    }

                    // Navigate to projects page
                    await this.navigateToProjects(page);

                    // CRITICAL: Open property modal by clicking Filter Properties
                    await this.openPropertyModal(page);

                    // Extract property data from modal
                    const properties = await this.extractPropertyData(page);

                    // Prepare results
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
                            approach: 'modal-extraction'
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
                            scraperVersion: '1.0.4',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            approach: 'modal-extraction',
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

                    this.logger.info('Modal-aware scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart)
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Modal-aware scraping workflow failed', {
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
                        approach: 'modal-extraction'
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
 * MAIN MODAL-AWARE ACTOR ENTRY POINT
 */
async function main() {
    try {
        await Actor.init();
        console.log('Modal-aware Actor initialized successfully');

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

        // Initialize modal-aware scraper
        console.log('Initializing modal-aware enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute modal-aware scraping workflow
        console.log('Starting modal-aware scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Modal-aware scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Modal-aware scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped
                });
            }
        } else {
            console.error('Modal-aware scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Modal-aware scraping failed', { error: results.error });
            }
            await Actor.fail(`Modal-aware scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in modal-aware actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in modal-aware actor', { 
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
