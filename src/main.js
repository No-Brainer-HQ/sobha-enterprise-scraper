/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.2 - FIXED SALESFORCE LIGHTNING LOADING
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
    // Performance settings - ENHANCED FOR LIGHTNING LOADING
    MAX_CONCURRENT_REQUESTS: 3, // Reduced for more stable loading
    REQUEST_TIMEOUT: 900000, // Increased to 15 minutes for Lightning loading
    NAVIGATION_TIMEOUT: 180000, // Increased to 3 minutes
    LIGHTNING_TIMEOUT: 120000, // New: 2 minutes for Lightning initialization
    
    // Security settings
    MAX_RETRY_ATTEMPTS: 5, // Increased for Lightning loading retries
    BASE_DELAY: 3000, // Increased for better stability
    MAX_DELAY: 15000,
    
    // Monitoring thresholds
    MIN_SUCCESS_RATE: 95.0,
    MAX_MEMORY_MB: 4096,
    
    // Portal endpoints
    LOGIN_URL: 'https://www.sobhapartnerportal.com/partnerportal/s/',
    
    // Selectors (ENHANCED for Lightning loading)
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]',
        
        // Lightning loading indicators
        lightningSpinners: '.slds-spinner, .loading, [class*="loading"], [class*="spinner"]',
        lightningError: '.auraErrorBox, .slds-notify_alert, [class*="error"]',
        
        // Enhanced dashboard detection
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
            requestDelay: input.requestDelay || 3.0, // Increased default delay
            retryAttempts: input.retryAttempts || 5, // Increased default retries
            enableStealth: input.enableStealth !== false,
            downloadDocuments: input.downloadDocuments || false,
            parallelRequests: input.parallelRequests || 2 // Reduced for stability
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
     * ENHANCED: Wait for Salesforce Lightning to fully load and initialize
     */
    async waitForLightningToLoad(page) {
        try {
            this.logger.info('Waiting for Salesforce Lightning framework to fully load');

            // Step 1: Wait for basic page load
            await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
            this.logger.debug('DOM content loaded');

            // Step 2: Wait for Lightning spinners to disappear
            this.logger.debug('Checking for Lightning loading spinners');
            try {
                await page.waitForSelector(CONFIG.SELECTORS.lightningSpinners, { timeout: 5000 });
                this.logger.info('Loading spinners detected, waiting for them to disappear');
                
                // Wait for all spinners to be hidden/removed
                await page.waitForFunction(() => {
                    const spinners = document.querySelectorAll('.slds-spinner, .loading, [class*="loading"], [class*="spinner"]');
                    return Array.from(spinners).every(spinner => 
                        !spinner.offsetParent || 
                        spinner.style.display === 'none' || 
                        spinner.hidden ||
                        !spinner.isConnected
                    );
                }, {}, { timeout: CONFIG.LIGHTNING_TIMEOUT });
                
                this.logger.info('✅ All loading spinners have disappeared');
            } catch (spinnerError) {
                this.logger.debug('No loading spinners found or already hidden');
            }

            // Step 3: Wait for Lightning framework initialization
            this.logger.debug('Waiting for Lightning framework initialization');
            await page.waitForFunction(() => {
                // Check for Aura framework
                if (window.$A && window.$A.getCallback) {
                    console.log('Aura framework detected');
                    return true;
                }
                
                // Check for Lightning base components
                if (window.LightningElement || window.lightning) {
                    console.log('Lightning framework detected');
                    return true;
                }
                
                // Check for Salesforce global objects
                if (window.Sfdc || window.sforce) {
                    console.log('Salesforce framework detected');
                    return true;
                }
                
                // Check for rendered Lightning components in DOM
                const lightningComponents = document.querySelectorAll(
                    '[class*="slds"], [class*="lightning"], [data-aura-rendered-by], c-*, lightning-*'
                );
                
                if (lightningComponents.length > 10) {
                    console.log(`Found ${lightningComponents.length} Lightning components`);
                    return true;
                }
                
                // Check if page is no longer showing "Loading" text
                const bodyText = document.body.textContent || '';
                const hasLoadingText = bodyText.includes('Loading') || 
                                     bodyText.includes('Sorry to interrupt') ||
                                     bodyText.includes('CSS Error');
                
                if (!hasLoadingText && bodyText.length > 1000) {
                    console.log('Page appears to be fully loaded (no loading text, substantial content)');
                    return true;
                }
                
                console.log('Lightning framework not yet initialized, waiting...');
                return false;
            }, {}, { timeout: CONFIG.LIGHTNING_TIMEOUT });

            this.logger.info('✅ Lightning framework initialization complete');

            // Step 4: Additional wait for component rendering
            this.logger.debug('Waiting for Lightning components to render');
            await page.waitForTimeout(5000); // Allow components to render

            // Step 5: Check for Lightning errors
            try {
                const errorElements = await page.locator(CONFIG.SELECTORS.lightningError).count();
                if (errorElements > 0) {
                    const errorText = await page.locator(CONFIG.SELECTORS.lightningError).first().textContent();
                    this.logger.warn('Lightning error detected on page', { errorText });
                }
            } catch (errorCheckError) {
                this.logger.debug('No Lightning errors found');
            }

            // Step 6: Final validation
            const finalPageState = await page.evaluate(() => {
                const bodyText = document.body.textContent || '';
                return {
                    hasLoadingText: bodyText.includes('Loading') || bodyText.includes('Sorry to interrupt'),
                    contentLength: bodyText.length,
                    lightningElements: document.querySelectorAll('[class*="slds"], [class*="lightning"]').length,
                    hasError: bodyText.includes('CSS Error') || bodyText.includes('JavaScript Error'),
                    title: document.title
                };
            });

            this.logger.info('Lightning loading validation complete', finalPageState);

            if (finalPageState.hasLoadingText || finalPageState.hasError) {
                throw new Error('Page still showing loading state or errors after Lightning initialization');
            }

            return true;

        } catch (error) {
            this.logger.error('Lightning loading failed', { error: error.message });
            throw new Error(`Failed to wait for Lightning to load: ${error.message}`);
        }
    }

    /**
     * ENHANCED: Authentication with Lightning loading support
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

                // ENHANCED: Wait for post-login page with Lightning loading
                this.logger.info('Waiting for post-login navigation and Lightning initialization');
                
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

                    // CRITICAL: Wait for Lightning to fully load
                    await this.waitForLightningToLoad(page);

                    // FIXED: Dismiss the promotional modal AFTER Lightning loads
                    await this.dismissPostLoginModal(page);
                    
                    // Additional wait to ensure modal dismissal and Lightning stability
                    await page.waitForTimeout(5000);

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
                    const delay = 5000 + Math.random() * 5000; // 5-10 seconds
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
     * ENHANCED: Modal dismissal with Lightning-aware approach
     */
    async dismissPostLoginModal(page) {
        try {
            this.logger.info('Checking for post-login promotional modal');

            // Wait for Lightning to be stable before modal detection
            await page.waitForTimeout(3000);

            // Check if the modal exists with extended detection
            const modalSelectors = [
                '.slds-modal.slds-fade-in-open',
                '.slds-modal.slds-modal_full',
                '[role="dialog"][aria-modal="true"]',
                'section[role="dialog"]'
            ];

            let modalExists = false;
            let modalSelector = '';
            for (const selector of modalSelectors) {
                const count = await page.locator(selector).count();
                if (count > 0) {
                    modalExists = true;
                    modalSelector = selector;
                    this.logger.info(`Modal detected with selector: ${selector}`);
                    break;
                }
            }
            
            if (modalExists) {
                this.logger.info('Promotional modal detected, attempting to close it with Lightning-aware methods');

                // Enhanced Method 1: Lightning-specific close buttons
                const lightningCloseSelectors = [
                    '.slds-modal button[data-key="close"]',
                    '.slds-modal lightning-button-icon[data-key="close"]',
                    '.slds-modal [class*="close"]',
                    '.slds-modal button[title*="Close"]',
                    '.slds-modal button[aria-label*="Close"]',
                    '.slds-modal button[aria-label*="close"]',
                    '.slds-modal .slds-modal__close',
                    '.slds-modal button:has-text("×")',
                    '.slds-modal button:has-text("✕")',
                    '.slds-modal header button',
                    '.slds-modal .slds-modal__header button'
                ];

                let modalClosed = false;
                for (const selector of lightningCloseSelectors) {
                    try {
                        this.logger.debug(`Trying Lightning close button: ${selector}`);
                        
                        const closeButton = page.locator(selector).first();
                        const isVisible = await closeButton.isVisible().catch(() => false);
                        
                        if (isVisible) {
                            this.logger.info(`Found close button: ${selector}`);
                            
                            // Try Playwright click first
                            await closeButton.click({ timeout: 5000 });
                            
                            // Wait for modal to disappear
                            await page.waitForSelector(modalSelector, { 
                                state: 'detached', 
                                timeout: 10000 
                            });
                            
                            this.logger.info('✅ Modal successfully closed with close button');
                            modalClosed = true;
                            break;
                        }
                    } catch (error) {
                        this.logger.debug(`Close selector failed: ${selector}`, { error: error.message });
                    }
                }

                // Method 2: Lightning-aware JavaScript dismissal
                if (!modalClosed) {
                    this.logger.info('Trying Lightning-aware JavaScript modal dismissal');
                    try {
                        await page.evaluate(() => {
                            console.log('Starting Lightning-aware modal removal...');
                            
                            // First, try to trigger Lightning's modal close event
                            const modalElements = document.querySelectorAll('.slds-modal, [role="dialog"]');
                            modalElements.forEach(modal => {
                                // Try to find and click close button programmatically
                                const closeButtons = modal.querySelectorAll(
                                    'button[data-key="close"], button[title*="Close"], button[aria-label*="close"], .slds-modal__close, button'
                                );
                                
                                closeButtons.forEach(button => {
                                    if (button.offsetParent && !button.disabled) {
                                        console.log('Attempting to click close button:', button);
                                        try {
                                            button.click();
                                        } catch (clickError) {
                                            console.log('Click failed, trying dispatchEvent');
                                            button.dispatchEvent(new MouseEvent('click', {
                                                view: window,
                                                bubbles: true,
                                                cancelable: true
                                            }));
                                        }
                                    }
                                });
                            });
                            
                            // If no close buttons worked, remove modal elements
                            setTimeout(() => {
                                const modalsToRemove = document.querySelectorAll('.slds-modal, [role="dialog"], .slds-backdrop');
                                modalsToRemove.forEach(element => {
                                    if (element.parentNode) {
                                        console.log('Removing modal element:', element.className);
                                        element.parentNode.removeChild(element);
                                    }
                                });
                                
                                // Reset body styles
                                document.body.style.overflow = 'auto';
                                document.body.style.pointerEvents = 'auto';
                                document.documentElement.style.overflow = 'auto';
                            }, 1000);
                            
                            return 'Lightning modal dismissal attempted';
                        });
                        
                        // Wait for Lightning to process the dismissal
                        await page.waitForTimeout(3000);
                        
                        // Check if modal is gone
                        const stillExists = await page.locator(modalSelector).count();
                        if (stillExists === 0) {
                            this.logger.info('✅ Lightning-aware modal dismissal successful');
                            modalClosed = true;
                        }
                        
                    } catch (jsError) {
                        this.logger.debug('Lightning modal dismissal failed', { error: jsError.message });
                    }
                }

                // Method 3: Escape key (Lightning should respond)
                if (!modalClosed) {
                    this.logger.info('Trying to close modal with Escape key');
                    try {
                        for (let i = 0; i < 3; i++) {
                            await page.keyboard.press('Escape');
                            await page.waitForTimeout(1000);
                        }
                        
                        await page.waitForSelector(modalSelector, { 
                            state: 'detached', 
                            timeout: 5000 
                        });
                        this.logger.info('✅ Modal closed with Escape key');
                        modalClosed = true;
                    } catch (escapeError) {
                        this.logger.debug('Escape key failed to close modal');
                    }
                }

                if (!modalClosed) {
                    this.logger.warn('Could not close promotional modal, attempting to continue anyway');
                }

                // Final wait for Lightning to stabilize
                await page.waitForTimeout(3000);

            } else {
                this.logger.info('No promotional modal detected');
            }

        } catch (error) {
            this.logger.warn('Error while dismissing post-login modal', { error: error.message });
            // Continue execution even if modal dismissal fails
        }
    }

    /**
     * ENHANCED: Navigate to Projects with Lightning-aware approach
     */
    async navigateToProjects(page) {
        try {
            this.logger.info('Navigating to Sobha Projects page with Lightning support');

            // Wait for Lightning to be stable
            await page.waitForTimeout(3000);

            // Method 1: Try direct URL navigation first (most reliable for Lightning)
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
                
                // Wait for Lightning to load on the new page
                await this.waitForLightningToLoad(page);
                
                this.logger.info('✅ Direct navigation to projects page successful');
                return true;
                
            } catch (directNavError) {
                this.logger.warn('Direct navigation failed, trying element-based navigation', { 
                    error: directNavError.message 
                });
            }

            // Method 2: Analyze available elements after Lightning loading
            const availableElements = await page.evaluate(() => {
                const elements = Array.from(document.querySelectorAll('a, button, [role="button"], [data-navigate]'));
                return elements.map(el => ({
                    text: el.textContent?.trim() || '',
                    href: el.href || '',
                    className: el.className || '',
                    id: el.id || '',
                    visible: el.offsetParent !== null && !el.hidden,
                    dataNavigate: el.getAttribute('data-navigate') || '',
                    onclick: el.onclick ? 'has onclick' : ''
                })).filter(el => el.visible && (el.text || el.href || el.dataNavigate));
            });

            this.logger.info('Available navigation elements after Lightning load:', { 
                elementCount: availableElements.length,
                sample: availableElements.slice(0, 5)
            });

            // Method 3: Enhanced selectors for Lightning navigation
            const enhancedNavigationSelectors = [
                // Lightning navigation specific
                '[data-navigate*="sobha-project"]',
                '[data-page-reference*="sobha-project"]',
                'lightning-navigation-item-api[data-key*="project"]',
                
                // Standard selectors
                'a[href="/partnerportal/s/sobha-project"]',
                'a[href*="sobha-project"]',
                'a:has-text("Sobha Projects")',
                'button:has-text("Sobha Projects")',
                'text=Projects',
                '[title*="Projects"]',
                
                // Generic Lightning navigation
                'a[role="menuitem"]',
                'lightning-navigation-item',
                '.slds-nav a',
                
                // Broad selectors
                'a[href*="sobha"]',
                'a[href*="project"]'
            ];

            // Try each navigation selector
            let navigated = false;
            for (const selector of enhancedNavigationSelectors) {
                try {
                    this.logger.debug(`Trying navigation selector: ${selector}`);
                    
                    await page.waitForSelector(selector, { timeout: 5000 });
                    
                    const element = page.locator(selector).first();
                    const isVisible = await element.isVisible();
                    
                    if (isVisible) {
                        this.logger.info(`Found navigation element: ${selector}`);
                        
                        // Scroll into view and click
                        await element.scrollIntoViewIfNeeded();
                        await page.waitForTimeout(1000);
                        await element.click();
                        
                        // Wait for navigation and Lightning loading
                        await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                        await this.waitForLightningToLoad(page);
                        
                        this.logger.info(`✅ Successfully navigated using: ${selector}`);
                        navigated = true;
                        break;
                    }
                } catch (selectorError) {
                    this.logger.debug(`Navigation selector failed: ${selector}`, { error: selectorError.message });
                }
            }

            if (navigated) {
                // Validate we're on the correct page
                await page.waitForTimeout(3000);
                
                const currentUrl = page.url();
                const pageContent = await page.evaluate(() => document.body.textContent?.toLowerCase() || '');
                const hasProjectContent = pageContent.includes('project') || pageContent.includes('sobha');
                
                this.logger.info('Projects page navigation validation', {
                    currentUrl: currentUrl.substring(0, 100),
                    hasProjectContent,
                    contentLength: pageContent.length
                });

                return true;
            } else {
                this.logger.warn('Could not find projects navigation element, proceeding with current page');
                return true; // Continue anyway
            }

        } catch (error) {
            this.logger.error('Failed to navigate to projects page', { error: error.message });
            return true; // Continue with data extraction on current page
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
     * COMPLETELY ENHANCED: Extract property data with Lightning-specific methods
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting enhanced property data extraction for Lightning');

            // Ensure Lightning is fully loaded before extraction
            await this.waitForLightningToLoad(page);

            // Wait for any remaining modals to be dismissed
            await page.waitForTimeout(3000);

            // Enhanced page structure analysis for Lightning
            const lightningPageStructure = await page.evaluate(() => {
                return {
                    // Lightning-specific elements
                    lightningComponents: document.querySelectorAll('[class*="lightning-"], lightning-*').length,
                    sldsComponents: document.querySelectorAll('[class*="slds-"]').length,
                    auraComponents: document.querySelectorAll('[data-aura-rendered-by]').length,
                    
                    // Data elements
                    tables: document.querySelectorAll('table, [role="table"]').length,
                    grids: document.querySelectorAll('[role="grid"], [class*="datatable"]').length,
                    dataRows: document.querySelectorAll('tr, [role="row"], [data-row-key-value]').length,
                    listItems: document.querySelectorAll('li, .list-item, [role="listitem"]').length,
                    cards: document.querySelectorAll('.slds-card, [class*="card"]').length,
                    
                    // Content indicators
                    sobhaElements: document.querySelectorAll('[class*="sobha"], [id*="sobha"]').length,
                    propertyElements: document.querySelectorAll('[class*="property"], [class*="unit"]').length,
                    
                    // Lightning data indicators
                    recordElements: document.querySelectorAll('[data-record-id], [data-row-key-value]').length,
                    forceRecords: document.querySelectorAll('[force-*], [lightning-*]').length,
                    
                    // Page state
                    bodyTextLength: document.body.textContent?.length || 0,
                    hasLoadingText: (document.body.textContent || '').includes('Loading'),
                    hasErrorText: (document.body.textContent || '').includes('Error'),
                    
                    // Sample content for debugging
                    sampleText: (document.body.textContent || '').substring(0, 500)
                };
            });

            this.logger.info('Enhanced Lightning page structure analysis:', lightningPageStructure);

            let extractedData = [];

            // Enhanced Approach 1: Lightning Force Records and Data Tables
            if (lightningPageStructure.recordElements > 0 || lightningPageStructure.auraComponents > 0) {
                try {
                    this.logger.info('Attempting Lightning Force record extraction');
                    
                    extractedData = await page.evaluate((maxResults) => {
                        const results = [];
                        
                        // Lightning Force selectors
                        const forceSelectors = [
                            '[data-row-key-value]',
                            '[data-record-id]',
                            'lightning-datatable [data-row-key-value]',
                            'force-record-layout-item',
                            'lightning-record-view-form',
                            'lightning-record-edit-form',
                            '[class*="forceListViewManagerGrid"] tr',
                            '[class*="forceListViewManager"] tbody tr',
                            '.slds-table tbody tr',
                            'c-sobha-property-list-item',
                            'c-property-list-item',
                            '[c-sobhaprojectlist]',
                            '[c-propertylist]'
                        ];
                        
                        for (const selector of forceSelectors) {
                            const elements = document.querySelectorAll(selector);
                            console.log(`Lightning Force selector ${selector}: found ${elements.length} elements`);
                            
                            if (elements.length > 0) {
                                for (let i = 0; i < Math.min(elements.length, maxResults); i++) {
                                    const element = elements[i];
                                    
                                    // Extract data from various Lightning patterns
                                    const extractData = () => {
                                        // Method 1: Data attributes
                                        const dataAttributes = {};
                                        for (const attr of element.attributes) {
                                            if (attr.name.startsWith('data-')) {
                                                dataAttributes[attr.name] = attr.value;
                                            }
                                        }
                                        
                                        // Method 2: Lightning field values
                                        const fieldValues = {};
                                        const fields = element.querySelectorAll(
                                            'lightning-output-field, lightning-formatted-text, .slds-form-element__control, [data-output-element-id]'
                                        );
                                        
                                        fields.forEach(field => {
                                            const label = field.getAttribute('data-label') || 
                                                        field.querySelector('label')?.textContent || 
                                                        field.getAttribute('field-label');
                                            const value = field.textContent?.trim() || 
                                                        field.getAttribute('value') || 
                                                        field.getAttribute('data-value');
                                            
                                            if (label && value) {
                                                fieldValues[label.toLowerCase().replace(/\s+/g, '_')] = value;
                                            }
                                        });
                                        
                                        // Method 3: Table cells
                                        const cells = element.querySelectorAll('td, th, .slds-cell, [role="gridcell"]');
                                        const cellData = Array.from(cells).map(cell => cell.textContent?.trim()).filter(Boolean);
                                        
                                        // Method 4: Text extraction with patterns
                                        const text = element.textContent?.trim() || '';
                                        const patterns = {
                                            project: text.match(/(?:project|development):\s*([^,\n]+)/i)?.[1],
                                            unit: text.match(/(?:unit|apartment|flat)[\s#]*([a-z0-9\-]+)/i)?.[1],
                                            price: text.match(/(?:price|cost|aed|usd)[\s:]*([0-9,]+)/i)?.[1],
                                            area: text.match(/(\d+)\s*(?:sqft|sq\.?\s*ft|square\s*feet)/i)?.[1],
                                            bedrooms: text.match(/(\d+)\s*(?:bed|bedroom|br)/i)?.[1],
                                            floor: text.match(/(\d+)(?:st|nd|rd|th)?\s*floor/i)?.[1]
                                        };
                                        
                                        return {
                                            dataAttributes,
                                            fieldValues,
                                            cellData,
                                            patterns,
                                            rawText: text.substring(0, 200)
                                        };
                                    };
                                    
                                    const data = extractData();
                                    
                                    // Build property object from extracted data
                                    const property = {
                                        unitId: `lightning_${Date.now()}_${i}`,
                                        project: data.fieldValues.project || 
                                                data.patterns.project || 
                                                data.cellData[0] || 
                                                'Sobha Project',
                                        subProject: data.fieldValues.sub_project || data.cellData[1] || '',
                                        unitType: data.fieldValues.unit_type || 
                                                 data.patterns.bedrooms ? `${data.patterns.bedrooms} Bedroom` : '',
                                        floor: data.fieldValues.floor || data.patterns.floor || '',
                                        unitNo: data.fieldValues.unit_number || 
                                               data.patterns.unit || 
                                               data.cellData[2] || 
                                               `Unit-${i + 1}`,
                                        totalUnitArea: data.fieldValues.area || 
                                                      data.patterns.area ? `${data.patterns.area} sqft` : '',
                                        startingPrice: data.fieldValues.price || 
                                                      data.patterns.price ? `${data.patterns.price} AED` : '',
                                        availability: 'available',
                                        sourceUrl: window.location.href,
                                        extractionMethod: `Lightning-Force-${selector}`,
                                        lightningData: data,
                                        scrapedAt: new Date().toISOString()
                                    };
                                    
                                    // Only include if we have meaningful data
                                    if (property.project !== 'Sobha Project' || 
                                        property.unitNo !== `Unit-${i + 1}` || 
                                        data.rawText.length > 50) {
                                        results.push(property);
                                    }
                                }
                                
                                if (results.length > 0) {
                                    console.log(`Successfully extracted ${results.length} properties using Lightning Force selector: ${selector}`);
                                    break;
                                }
                            }
                        }
                        
                        return results;
                    }, this.input.maxResults);

                    this.logger.info('Lightning Force extraction completed', { propertiesFound: extractedData.length });

                } catch (lightningError) {
                    this.logger.warn('Lightning Force extraction failed', { error: lightningError.message });
                }
            }

            // Enhanced Approach 2: Lightning DataTable extraction
            if (extractedData.length === 0 && lightningPageStructure.grids > 0) {
                try {
                    this.logger.info('Attempting Lightning DataTable extraction');
                    
                    extractedData = await page.evaluate((maxResults) => {
                        const results = [];
                        const datatables = document.querySelectorAll('lightning-datatable, [role="grid"], .slds-table');
                        
                        datatables.forEach((table, tableIndex) => {
                            const rows = table.querySelectorAll('[role="row"]:not([role="row"]:first-child), tbody tr');
                            
                            for (let i = 0; i < Math.min(rows.length, maxResults - results.length); i++) {
                                const row = rows[i];
                                const cells = row.querySelectorAll('[role="gridcell"], td, .slds-cell');
                                
                                if (cells.length >= 3) {
                                    const cellTexts = Array.from(cells).map(cell => cell.textContent?.trim() || '');
                                    
                                    results.push({
                                        unitId: `datatable_${tableIndex}_${i}_${Date.now()}`,
                                        project: cellTexts[0] || 'Sobha DataTable Project',
                                        subProject: cellTexts[1] || '',
                                        unitType: cellTexts[2] || '',
                                        floor: cellTexts[3] || '',
                                        unitNo: cellTexts[4] || `DT-${i + 1}`,
                                        totalUnitArea: cellTexts[5] || '',
                                        startingPrice: cellTexts[6] || '',
                                        availability: 'available',
                                        sourceUrl: window.location.href,
                                        extractionMethod: 'Lightning-DataTable',
                                        rawCellData: cellTexts,
                                        scrapedAt: new Date().toISOString()
                                    });
                                }
                            }
                        });
                        
                        return results;
                    }, this.input.maxResults);

                    this.logger.info('Lightning DataTable extraction completed', { propertiesFound: extractedData.length });

                } catch (datatableError) {
                    this.logger.warn('Lightning DataTable extraction failed', { error: datatableError.message });
                }
            }

            // Enhanced Approach 3: Lightning Card extraction
            if (extractedData.length === 0 && lightningPageStructure.cards > 0) {
                try {
                    this.logger.info('Attempting Lightning Card extraction');
                    
                    extractedData = await page.evaluate((maxResults) => {
                        const results = [];
                        const cards = document.querySelectorAll('.slds-card, [class*="card"], lightning-card');
                        
                        for (let i = 0; i < Math.min(cards.length, maxResults); i++) {
                            const card = cards[i];
                            const text = card.textContent?.trim() || '';
                            
                            // Skip navigation cards
                            if (text.length > 100 && 
                                !text.includes('Dashboard') && 
                                !text.includes('Profile') &&
                                (text.includes('Sobha') || text.includes('Unit') || text.includes('Project'))) {
                                
                                results.push({
                                    unitId: `card_${i}_${Date.now()}`,
                                    project: text.includes('Sobha') ? 'Sobha Card Project' : 'Card Project',
                                    subProject: '',
                                    unitType: '',
                                    floor: '',
                                    unitNo: `Card-${i + 1}`,
                                    totalUnitArea: '',
                                    startingPrice: '',
                                    availability: 'available',
                                    rawData: text.substring(0, 300),
                                    sourceUrl: window.location.href,
                                    extractionMethod: 'Lightning-Card',
                                    scrapedAt: new Date().toISOString()
                                });
                            }
                        }
                        
                        return results;
                    }, this.input.maxResults);

                    this.logger.info('Lightning Card extraction completed', { propertiesFound: extractedData.length });

                } catch (cardError) {
                    this.logger.warn('Lightning Card extraction failed', { error: cardError.message });
                }
            }

            // Enhanced Approach 4: Smart content extraction with Lightning awareness
            if (extractedData.length === 0) {
                this.logger.info('Attempting enhanced content extraction');
                
                extractedData = await page.evaluate((maxResults) => {
                    const results = [];
                    
                    // Look for any meaningful content that's not navigation
                    const contentElements = document.querySelectorAll(
                        '[class*="property"], [class*="unit"], [class*="listing"], [class*="sobha"], ' +
                        '[id*="property"], [id*="unit"], [id*="sobha"], ' +
                        '.slds-card, .slds-tile, [data-name*="property"], [data-name*="unit"]'
                    );
                    
                    for (let i = 0; i < Math.min(contentElements.length, maxResults); i++) {
                        const element = contentElements[i];
                        const text = element.textContent?.trim() || '';
                        
                        // Enhanced filtering - look for property-related content
                        if (text.length > 50 && 
                            !text.includes('Dashboard') && 
                            !text.includes('Profile') && 
                            !text.includes('About Sobha') &&
                            !text.includes('Marketing') &&
                            !text.includes('Performance') &&
                            !text.includes('Loading') &&
                            (text.includes('Project') || 
                             text.includes('Unit') || 
                             text.includes('Bedroom') ||
                             text.includes('sqft') ||
                             text.includes('AED') ||
                             text.match(/\d+/) ||
                             element.className.includes('property') ||
                             element.className.includes('unit'))) {
                            
                            results.push({
                                unitId: `enhanced_${i}_${Date.now()}`,
                                project: 'Sobha Enhanced Content',
                                subProject: '',
                                unitType: '',
                                floor: '',
                                unitNo: `Enhanced-${i + 1}`,
                                totalUnitArea: '',
                                startingPrice: '',
                                availability: 'available',
                                rawData: text.substring(0, 500),
                                sourceUrl: window.location.href,
                                extractionMethod: 'Enhanced-Content',
                                elementDetails: {
                                    className: element.className,
                                    id: element.id,
                                    tagName: element.tagName
                                },
                                scrapedAt: new Date().toISOString()
                            });
                        }
                    }
                    
                    return results;
                }, this.input.maxResults);

                this.logger.info('Enhanced content extraction completed', { propertiesFound: extractedData.length });
            }

            // If still no meaningful data, create enhanced debug entry
            if (extractedData.length === 0) {
                this.logger.warn('No property data found - creating enhanced debug entry');
                
                const enhancedPageAnalysis = await page.evaluate(() => {
                    const analysis = {
                        url: window.location.href,
                        title: document.title,
                        
                        // Lightning framework state
                        hasAura: !!window.$A,
                        hasLightning: !!window.LightningElement,
                        hasSalesforce: !!window.Sfdc,
                        
                        // Content analysis
                        bodyText: document.body.textContent?.substring(0, 2000) || '',
                        
                        // Element counts
                        elementCounts: {
                            total: document.querySelectorAll('*').length,
                            divs: document.querySelectorAll('div').length,
                            spans: document.querySelectorAll('span').length,
                            tables: document.querySelectorAll('table').length,
                            cards: document.querySelectorAll('.slds-card, .card').length,
                            buttons: document.querySelectorAll('button').length,
                            links: document.querySelectorAll('a').length,
                            lightning: document.querySelectorAll('[class*="lightning-"], lightning-*').length,
                            slds: document.querySelectorAll('[class*="slds-"]').length,
                            aura: document.querySelectorAll('[data-aura-rendered-by]').length
                        },
                        
                        // Sample elements
                        sampleClasses: Array.from(document.querySelectorAll('[class]'))
                            .slice(0, 20)
                            .map(el => el.className)
                            .filter(Boolean),
                        
                        // Page state indicators
                        indicators: {
                            hasLoadingSpinner: document.querySelectorAll('.slds-spinner, [class*="loading"]').length > 0,
                            hasErrorMessage: document.body.textContent?.includes('Error') || false,
                            hasNoDataMessage: document.body.textContent?.includes('No records') || 
                                            document.body.textContent?.includes('No data') || false,
                            contentLoaded: document.body.textContent?.length > 1000
                        }
                    };
                    
                    return analysis;
                });
                
                extractedData = [{
                    unitId: `debug_lightning_${Date.now()}`,
                    project: 'Enhanced Debug Entry - Lightning Analysis',
                    subProject: 'No Properties Found',
                    unitType: 'Debug',
                    floor: '0',
                    unitNo: 'DEBUG-LIGHTNING-001',
                    totalUnitArea: '0',
                    startingPrice: '0',
                    availability: 'debug',
                    sourceUrl: enhancedPageAnalysis.url,
                    pageTitle: enhancedPageAnalysis.title,
                    debugInfo: {
                        message: 'No property data found after Lightning analysis',
                        lightningAnalysis: enhancedPageAnalysis,
                        pageStructure: lightningPageStructure,
                        extractionAttempts: ['Lightning-Force', 'Lightning-DataTable', 'Lightning-Card', 'Enhanced-Content']
                    },
                    extractionMethod: 'Enhanced-Debug-Analysis',
                    scrapedAt: new Date().toISOString()
                }];
            }

            // Validate and clean extracted data
            const validProperties = extractedData.filter(prop => 
                prop.unitId && prop.project && prop.unitNo
            );

            this.metrics.recordPropertiesScraped(validProperties.length);
            
            this.logger.info('Enhanced property data extraction completed', {
                totalExtracted: extractedData.length,
                validProperties: validProperties.length,
                invalidFiltered: extractedData.length - validProperties.length,
                extractionMethods: [...new Set(extractedData.map(p => p.extractionMethod))],
                lightningFrameworkState: lightningPageStructure
            });

            return validProperties;

        } catch (error) {
            this.logger.error('Enhanced property data extraction failed', { error: error.message });
            
            // Return enhanced error entry
            return [{
                unitId: `error_lightning_${Date.now()}`,
                project: 'Enhanced Error Entry',
                subProject: 'Extraction completely failed',
                unitType: 'Error',
                floor: '0',
                unitNo: 'ERROR-LIGHTNING-001',
                totalUnitArea: '0',
                startingPrice: '0',
                availability: 'error',
                sourceUrl: page.url(),
                errorInfo: {
                    message: error.message,
                    stack: error.stack,
                    timestamp: new Date().toISOString()
                },
                extractionMethod: 'Enhanced-Error-Fallback',
                scrapedAt: new Date().toISOString()
            }];
        }
    }

    /**
     * Main enterprise scraping workflow with Lightning support
     */
    async executeScraping() {
        const crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: 1,
            requestHandlerTimeoutSecs: CONFIG.REQUEST_TIMEOUT / 1000, // 15 minutes for Lightning
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
                        '--disable-blink-features=AutomationControlled',
                        '--disable-extensions',
                        '--disable-plugins'
                    ]
                }
            },
            requestHandler: async ({ page, request }) => {
                const scrapeStart = performance.now();
                
                try {
                    this.logger.info('Starting enhanced Lightning-aware scraping workflow', { url: request.url });

                    // Memory monitoring
                    this.metrics.recordMemoryUsage();

                    // Apply rate limiting
                    await this.rateLimiter.wait();

                    // Perform authentication with Lightning support
                    if (!await this.authenticate(page)) {
                        throw new Error('Authentication failed');
                    }

                    // Navigate to projects page with Lightning awareness
                    await this.navigateToProjects(page);

                    // Apply filters (if any)
                    await this.applyFilters(page);

                    // Extract property data with Lightning methods
                    const properties = await this.extractPropertyData(page);

                    // Prepare enhanced enterprise output
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
                            lightningSupport: true,
                            enhancedExtraction: true
                        },
                        
                        // Results data
                        summary: {
                            totalProperties: properties.length,
                            successRate: this.metrics.getSuccessRate(),
                            scrapingDuration: Math.round(performance.now() - scrapeStart),
                            lightningFrameworkDetected: true
                        },
                        
                        // Property data
                        properties,
                        
                        // Enterprise metrics
                        metrics: this.metrics.getSummary(),
                        
                        // Enhanced metadata
                        metadata: {
                            scraperVersion: '1.0.2',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            lightningSupport: true,
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

                    this.logger.info('Enhanced Lightning-aware scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart),
                        lightningSupport: true
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Enhanced scraping workflow failed', {
                        error: error.message,
                        duration: Math.round(duration),
                        stack: error.stack
                    });
                    
                    // Store enhanced error results
                    await Dataset.pushData({
                        sessionId: this.sessionId,
                        timestamp: new Date().toISOString(),
                        success: false,
                        error: {
                            message: error.message,
                            stack: error.stack,
                            type: 'Lightning-Enhanced-Error'
                        },
                        metrics: this.metrics.getSummary(),
                        lightningSupport: true
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
            metrics: this.metrics.getSummary(),
            lightningSupport: true
        };
    }
}

/**
 * MAIN ENTERPRISE ACTOR ENTRY POINT
 */
async function main() {
    try {
        await Actor.init();
        console.log('Enhanced Lightning-aware Actor initialized successfully');

        // Get and validate input
        const actorInput = await Actor.getInput() ?? {};
        console.log('Actor input received:', { hasEmail: !!actorInput.email, hasPassword: !!actorInput.password });
        
        // Enhanced input validation
        let validatedInput;
        try {
            validatedInput = InputValidator.validate(actorInput);
            console.log('Enhanced input validation successful');
        } catch (validationError) {
            console.error('Input validation failed:', validationError.message);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Input validation failed', { error: validationError.message });
            }
            await Actor.fail(`Input validation failed: ${validationError.message}`);
            return;
        }

        // Initialize enhanced Lightning-aware scraper
        console.log('Initializing enhanced Lightning-aware enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute enhanced scraping workflow
        console.log('Starting enhanced Lightning-aware scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Enhanced Lightning-aware scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Enhanced Lightning-aware scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped,
                    lightningSupport: true
                });
            }
        } else {
            console.error('Enhanced scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Enhanced Lightning-aware scraping failed', { error: results.error });
            }
            await Actor.fail(`Enhanced scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in enhanced Lightning-aware actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in enhanced Lightning-aware actor', { 
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
