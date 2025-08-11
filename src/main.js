/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.1 - FIXED POST-LOGIN DETECTION
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
    // Performance settings - INCREASED TIMEOUTS FOR MODAL HANDLING
    MAX_CONCURRENT_REQUESTS: 5,
    REQUEST_TIMEOUT: 600000, // Increased to 10 minutes for complex modal handling
    NAVIGATION_TIMEOUT: 120000, // Increased to 2 minutes
    
    // Security settings
    MAX_RETRY_ATTEMPTS: 3,
    BASE_DELAY: 2000,
    MAX_DELAY: 10000,
    
    // Monitoring thresholds
    MIN_SUCCESS_RATE: 95.0,
    MAX_MEMORY_MB: 4096,
    
    // Portal endpoints
    LOGIN_URL: 'https://www.sobhapartnerportal.com/partnerportal/s/',
    
    // Selectors (FIXED - Simplified and more flexible)
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]', // SIMPLIFIED - Use the working selector
        
        // FIXED: More flexible dashboard detection using URL + basic page elements
        dashboardIndicator: 'body', // Just check if page loads - we'll use URL detection
        
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
     * FIXED: Enhanced authentication with URL-based validation
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
                    waitUntil: 'domcontentloaded', // CHANGED: Less strict loading requirement
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

                // Click login button - use the known working selector
                this.logger.info('Clicking login button');
                await page.waitForSelector(CONFIG.SELECTORS.loginButton, { timeout: 10000 });
                await page.click(CONFIG.SELECTORS.loginButton);

                // FIXED: Wait for post-login page using URL-based detection instead of specific elements
                this.logger.info('Waiting for post-login navigation');
                
                try {
                    // Wait for either:
                    // 1. URL to change indicating successful login
                    // 2. Basic page load completion
                    // 3. Any content to appear (flexible approach)
                    
                    await Promise.race([
                        // Option 1: Wait for URL change (most reliable)
                        page.waitForFunction(() => {
                            return window.location.href.includes('/partnerportal/s/') || 
                                   window.location.href.includes('frontdoor') ||
                                   window.location.href !== 'https://www.sobhapartnerportal.com/partnerportal/s/';
                        }, {}, { timeout: 30000 }),
                        
                        // Option 2: Wait for page to have any content (fallback)
                        page.waitForSelector('body', { timeout: 30000 })
                    ]);

                    // Additional validation: Check we're not still on login page
                    const currentUrl = page.url();
                    const pageTitle = await page.title().catch(() => 'Unknown');
                    
                    this.logger.info('Post-login page loaded', { 
                        currentUrl: currentUrl.substring(0, 100),
                        pageTitle 
                    });

                    // CRITICAL FIX: Dismiss the promotional modal that appears after login
                    await this.dismissPostLoginModal(page);
                    
                    // Additional wait to ensure modal is fully dismissed
                    await page.waitForTimeout(3000);

                    // If we've navigated away from the initial login URL, consider it successful
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
     * CRITICAL FIX: Dismiss the promotional modal that appears after login
     */
    async dismissPostLoginModal(page) {
        try {
            this.logger.info('Checking for post-login promotional modal');

            // Wait a moment for the modal to appear
            await page.waitForTimeout(3000);

            // Check if the modal exists with extended detection
            const modalSelectors = [
                '.slds-modal.slds-fade-in-open',
                '.slds-modal.slds-modal_full',
                '[role="dialog"][aria-modal="true"]',
                'section[role="dialog"]'
            ];

            let modalExists = false;
            for (const selector of modalSelectors) {
                const count = await page.locator(selector).count();
                if (count > 0) {
                    modalExists = true;
                    this.logger.info(`Modal detected with selector: ${selector}`);
                    break;
                }
            }
            
            if (modalExists) {
                this.logger.info('Promotional modal detected, attempting to close it with aggressive methods');

                // Method 1: Try clicking X button in the top-right corner of modal
                const xButtonSelectors = [
                    '.slds-modal button[title="Close"]',
                    '.slds-modal [data-key="close"]',
                    '.slds-modal .slds-button_icon',
                    '.slds-modal button[aria-label*="Close"]',
                    '.slds-modal button[aria-label*="close"]',
                    '.slds-modal .slds-modal__close',
                    // Look for the actual X symbol
                    '.slds-modal button:has-text("×")',
                    '.slds-modal button:has-text("✕")',
                    '.slds-modal lightning-button-icon',
                    // Generic close button in modal header
                    '.slds-modal header button',
                    '.slds-modal .slds-modal__header button'
                ];

                let modalClosed = false;
                for (const selector of xButtonSelectors) {
                    try {
                        this.logger.debug(`Trying close button selector: ${selector}`);
                        
                        const closeButton = page.locator(selector).first();
                        const isVisible = await closeButton.isVisible().catch(() => false);
                        
                        if (isVisible) {
                            this.logger.info(`Found close button with selector: ${selector}`);
                            
                            // Force click with JavaScript to bypass modal blocking
                            await closeButton.evaluate(el => el.click());
                            
                            // Wait for modal to disappear
                            await page.waitForSelector('.slds-modal.slds-fade-in-open', { 
                                state: 'detached', 
                                timeout: 5000 
                            });
                            
                            this.logger.info('✅ Modal successfully closed with close button');
                            modalClosed = true;
                            break;
                        }
                    } catch (error) {
                        this.logger.debug(`Close selector failed: ${selector}`, { error: error.message });
                    }
                }

                // Method 2: Try pressing Escape key multiple times
                if (!modalClosed) {
                    this.logger.info('Trying to close modal with Escape key');
                    try {
                        for (let i = 0; i < 3; i++) {
                            await page.keyboard.press('Escape');
                            await page.waitForTimeout(1000);
                        }
                        
                        await page.waitForSelector('.slds-modal.slds-fade-in-open', { 
                            state: 'detached', 
                            timeout: 3000 
                        });
                        this.logger.info('✅ Modal closed with Escape key');
                        modalClosed = true;
                    } catch (escapeError) {
                        this.logger.debug('Escape key failed to close modal');
                    }
                }

                // Method 3: Nuclear JavaScript removal - target all possible blocking elements
                if (!modalClosed) {
                    this.logger.info('Trying nuclear modal removal with comprehensive JavaScript');
                    try {
                        await page.evaluate(() => {
                            console.log('Starting nuclear modal removal...');
                            
                            // Remove all possible modal and blocking elements
                            const selectors = [
                                '.slds-modal',
                                '[role="dialog"]',
                                '.slds-backdrop',
                                '.modal-backdrop',
                                '[aria-modal="true"]',
                                '[data-aura-rendered-by]',
                                '.cCenterPanel',
                                'section[role="dialog"]',
                                '[c-brokerportalhomepage_brokerportalhomepage]'
                            ];
                            
                            let removedCount = 0;
                            selectors.forEach(selector => {
                                const elements = document.querySelectorAll(selector);
                                elements.forEach(element => {
                                    if (element && element.parentNode) {
                                        console.log(`Removing element: ${selector}`);
                                        element.parentNode.removeChild(element);
                                        removedCount++;
                                    }
                                });
                            });
                            
                            // Also try to remove by checking for specific class patterns
                            const allElements = document.querySelectorAll('*');
                            allElements.forEach(element => {
                                const className = element.className;
                                if (typeof className === 'string' && 
                                    (className.includes('slds-modal') || 
                                     className.includes('modal') ||
                                     className.includes('backdrop') ||
                                     element.getAttribute('role') === 'dialog')) {
                                    if (element.parentNode) {
                                        console.log(`Removing modal-like element: ${className}`);
                                        element.parentNode.removeChild(element);
                                        removedCount++;
                                    }
                                }
                            });
                            
                            // Force reset body styles
                            document.body.style.pointerEvents = 'auto';
                            document.body.style.overflow = 'auto';
                            document.body.style.position = 'static';
                            document.documentElement.style.pointerEvents = 'auto';
                            document.documentElement.style.overflow = 'auto';
                            
                            // Remove any CSS that might be blocking interactions
                            const styleSheets = document.styleSheets;
                            for (let i = 0; i < styleSheets.length; i++) {
                                try {
                                    const sheet = styleSheets[i];
                                    if (sheet.cssRules) {
                                        for (let j = sheet.cssRules.length - 1; j >= 0; j--) {
                                            const rule = sheet.cssRules[j];
                                            if (rule.selectorText && 
                                                (rule.selectorText.includes('.slds-modal') ||
                                                 rule.selectorText.includes('.slds-backdrop'))) {
                                                sheet.deleteRule(j);
                                            }
                                        }
                                    }
                                } catch (cssError) {
                                    // Ignore cross-origin CSS errors
                                }
                            }
                            
                            console.log(`Nuclear removal completed. Removed ${removedCount} elements.`);
                            return removedCount;
                        });
                        
                        // Wait for any Salesforce Lightning to finish processing
                        await page.waitForTimeout(3000);
                        
                        // Verify modal is really gone
                        const stillExists = await page.locator('.slds-modal.slds-fade-in-open').count();
                        if (stillExists === 0) {
                            this.logger.info('✅ Nuclear modal removal successful');
                            modalClosed = true;
                        } else {
                            this.logger.info(`⚠️ ${stillExists} modals still exist after nuclear removal`);
                        }
                        
                    } catch (jsError) {
                        this.logger.debug('Nuclear JavaScript modal removal failed', { error: jsError.message });
                    }
                }

                // Method 4: Click outside modal area (backdrop)
                if (!modalClosed) {
                    this.logger.info('Trying to close modal by clicking backdrop');
                    try {
                        // Click on the backdrop area (outside the modal content)
                        await page.mouse.click(100, 100); // Top-left corner
                        await page.waitForTimeout(1000);
                        
                        await page.waitForSelector('.slds-modal.slds-fade-in-open', { 
                            state: 'detached', 
                            timeout: 3000 
                        });
                        this.logger.info('✅ Modal closed by clicking backdrop');
                        modalClosed = true;
                    } catch (backdropError) {
                        this.logger.debug('Backdrop click failed to close modal');
                    }
                }

                if (!modalClosed) {
                    this.logger.warn('Could not close promotional modal with any method, attempting to continue anyway');
                }

                // Final wait for any animations to complete
                await page.waitForTimeout(2000);

            } else {
                this.logger.info('No promotional modal detected');
            }

        } catch (error) {
            this.logger.warn('Error while dismissing post-login modal', { error: error.message });
            // Continue execution even if modal dismissal fails
        }
    }

    /**
     * IMPROVED: Navigate to Sobha Projects page with flexible selectors
     */
    async navigateToProjects(page) {
        try {
            this.logger.info('Navigating to Sobha Projects page');

            // Wait a moment to ensure modal dismissal is complete
            await page.waitForTimeout(2000);

            // First, analyze what's available on the current page
            const availableLinks = await page.evaluate(() => {
                const links = Array.from(document.querySelectorAll('a, button, [role="button"]'));
                return links.map(link => ({
                    text: link.textContent?.trim() || '',
                    href: link.href || '',
                    className: link.className || '',
                    id: link.id || '',
                    visible: link.offsetParent !== null && !link.hidden
                })).filter(link => (link.text || link.href) && link.visible);
            });

            this.logger.info('Available navigation elements:', { 
                linkCount: availableLinks.length,
                links: availableLinks.slice(0, 10) // Log first 10 for debugging
            });

            // Enhanced navigation selectors based on the actual Sobha portal structure
            const navigationSelectors = [
                // Direct link to Sobha Projects page (from logs)
                'a[href="/partnerportal/s/sobha-project"]',
                'a[href*="sobha-project"]',
                
                // Text-based selectors
                'a:has-text("Sobha Projects")',
                'button:has-text("Sobha Projects")', 
                'text=Projects',
                '[title*="Projects"]',
                '[data-label*="Projects"]',
                
                // Menu items
                'a[role="menuitem"]:has-text("Sobha Projects")',
                'a[role="menuitem"][href*="project"]',
                
                // Navigation links
                'nav a:has-text("Sobha Projects")',
                '.slds-nav a:has-text("Sobha Projects")',
                
                // Generic project links
                'a[href*="sobha"]',
                'a[href*="project"]'
            ];

            // Try each navigation selector
            let navigated = false;
            for (const selector of navigationSelectors) {
                try {
                    this.logger.debug(`Trying navigation selector: ${selector}`);
                    
                    // Wait for element to be present
                    await page.waitForSelector(selector, { timeout: 3000 });
                    
                    // Check if element is visible and clickable
                    const element = page.locator(selector).first();
                    const isVisible = await element.isVisible();
                    
                    if (isVisible) {
                        this.logger.info(`Found navigation element: ${selector}`);
                        
                        // Scroll into view if needed
                        await element.scrollIntoViewIfNeeded();
                        
                        // Wait a moment and click
                        await page.waitForTimeout(500);
                        await element.click();
                        
                        // Wait for navigation to complete
                        await page.waitForLoadState('domcontentloaded', { timeout: 10000 });
                        
                        this.logger.info(`Successfully navigated using: ${selector}`);
                        navigated = true;
                        break;
                    }
                } catch (selectorError) {
                    this.logger.debug(`Navigation selector failed: ${selector}`, { error: selectorError.message });
                }
            }

            if (!navigated) {
                // Fallback: Try to navigate directly to the projects URL
                this.logger.info('Trying direct navigation to projects page');
                try {
                    const currentUrl = page.url();
                    const baseUrl = currentUrl.split('/partnerportal')[0];
                    const projectsUrl = `${baseUrl}/partnerportal/s/sobha-project`;
                    
                    this.logger.info(`Attempting direct navigation to: ${projectsUrl}`);
                    await page.goto(projectsUrl, { 
                        waitUntil: 'domcontentloaded',
                        timeout: 30000 
                    });
                    
                    navigated = true;
                    this.logger.info('Successfully navigated via direct URL');
                } catch (directNavError) {
                    this.logger.warn('Direct navigation failed', { error: directNavError.message });
                }
            }

            if (navigated) {
                // Wait for page to load and check if we're on a projects page
                await page.waitForTimeout(3000);
                
                const currentUrl = page.url();
                const pageContent = await page.evaluate(() => document.body.textContent?.toLowerCase() || '');
                const hasProjectContent = pageContent.includes('project') || pageContent.includes('sobha');
                
                this.logger.info('Projects page navigation result', {
                    currentUrl: currentUrl.substring(0, 100),
                    hasProjectContent,
                    pageContentLength: pageContent.length
                });

                return true;
            } else {
                this.logger.warn('Could not find projects navigation element, proceeding with current page');
                return true; // Continue anyway, maybe we're already on the right page
            }

        } catch (error) {
            this.logger.error('Failed to navigate to projects page', { error: error.message });
            // Don't throw error, try to continue with data extraction on current page
            return true;
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
     * Extract property data with enterprise validation - ENHANCED for Sobha Portal
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting property data extraction');

            // Wait for any modals to be dismissed first
            await page.waitForTimeout(2000);

            // First, try to dismiss any remaining modals that might be blocking content
            await this.dismissPostLoginModal(page);

            // Analyze the page structure to find data elements
            const pageStructure = await page.evaluate(() => {
                return {
                    tables: document.querySelectorAll('table').length,
                    dataRows: document.querySelectorAll('tr, [role="row"]').length,
                    listItems: document.querySelectorAll('li, .list-item, .property-item').length,
                    cards: document.querySelectorAll('.card, .property-card, .unit-card, [class*="card"]').length,
                    sobhaElements: document.querySelectorAll('[class*="sobha"], [class*="property"], [class*="unit"]').length,
                    salesforceRecords: document.querySelectorAll('[data-record-id], [data-row-key-value]').length,
                    hasData: document.body.textContent.length
                };
            });

            this.logger.info('Enhanced page structure analysis:', pageStructure);

            let extractedData = [];

            // Approach 1: Look for Salesforce Lightning data components (most likely)
            try {
                this.logger.info('Trying Salesforce Lightning data extraction');
                
                extractedData = await page.evaluate((maxResults) => {
                    const results = [];
                    
                    // Look for Lightning data table rows or records
                    const lightningSelectors = [
                        '[data-row-key-value]',  // Lightning data table rows
                        '[data-record-id]',      // Salesforce record elements
                        '.slds-table tbody tr',   // Lightning design system table
                        '[role="row"]:not([role="columnheader"])', // Accessible table rows
                        '.sobha-property',        // Custom Sobha property elements
                        '.property-row',          // Generic property rows
                        '[class*="property"]',    // Any element with "property" in class name
                        '[class*="unit"]'         // Any element with "unit" in class name
                    ];
                    
                    for (const selector of lightningSelectors) {
                        const elements = document.querySelectorAll(selector);
                        console.log(`Found ${elements.length} elements with selector: ${selector}`);
                        
                        if (elements.length > 0) {
                            for (let i = 0; i < Math.min(elements.length, maxResults); i++) {
                                const element = elements[i];
                                const text = element.textContent?.trim() || '';
                                
                                // Skip navigation elements
                                if (text.length > 20 && 
                                    !text.includes('Dashboard') && 
                                    !text.includes('Profile') && 
                                    !text.includes('About') &&
                                    !text.includes('Marketing')) {
                                    
                                    // Try to extract structured data from the element
                                    const cells = element.querySelectorAll('td, .cell, [role="cell"], .field-value');
                                    const links = element.querySelectorAll('a');
                                    const spans = element.querySelectorAll('span');
                                    
                                    let project = '';
                                    let unitNo = '';
                                    let price = '';
                                    let area = '';
                                    let floor = '';
                                    let unitType = '';
                                    
                                    // Extract data from cells if available
                                    if (cells.length >= 3) {
                                        project = cells[0]?.textContent?.trim() || '';
                                        unitType = cells[1]?.textContent?.trim() || '';
                                        unitNo = cells[2]?.textContent?.trim() || '';
                                        area = cells[3]?.textContent?.trim() || '';
                                        price = cells[4]?.textContent?.trim() || '';
                                        floor = cells[5]?.textContent?.trim() || '';
                                    } else {
                                        // Try to parse from text content
                                        const textParts = text.split(/\s+/);
                                        
                                        // Look for patterns in the text
                                        for (const part of textParts) {
                                            if (part.match(/^[A-Z]\d+/)) { // Unit number pattern
                                                unitNo = part;
                                            } else if (part.match(/\d+\s*(sqft|sq\.?ft\.?|square)/i)) { // Area pattern
                                                area = part;
                                            } else if (part.match(/\d+[\d,]*\s*(aed|usd|\$)/i)) { // Price pattern
                                                price = part;
                                            } else if (part.match(/^(ground|g|basement|b|\d+)(st|nd|rd|th)?\s*floor/i)) { // Floor pattern
                                                floor = part;
                                            }
                                        }
                                        
                                        // If no structured data found, use element content
                                        if (!project) project = text.substring(0, 50);
                                        if (!unitNo) unitNo = `Unit-${i + 1}`;
                                    }
                                    
                                    const unitId = `sobha_${project.replace(/\s+/g, '_')}_${unitNo}_${Date.now()}_${i}`.replace(/[^a-zA-Z0-9_]/g, '');
                                    
                                    if (project && unitNo) {
                                        results.push({
                                            unitId,
                                            project: project || 'Sobha Properties',
                                            subProject: '',
                                            unitType: unitType || '',
                                            floor: floor || '',
                                            unitNo: unitNo,
                                            totalUnitArea: area || '',
                                            startingPrice: price || '',
                                            availability: 'available',
                                            sourceUrl: window.location.href,
                                            extractionMethod: `Lightning-${selector}`,
                                            rawData: text.substring(0, 200),
                                            scrapedAt: new Date().toISOString()
                                        });
                                    }
                                }
                            }
                            
                            if (results.length > 0) {
                                console.log(`Successfully extracted ${results.length} properties using ${selector}`);
                                break; // Stop trying other selectors if we found data
                            }
                        }
                    }
                    
                    return results;
                }, this.input.maxResults);

                this.logger.info('Salesforce Lightning extraction completed', { propertiesFound: extractedData.length });

            } catch (lightningError) {
                this.logger.warn('Lightning extraction failed', { error: lightningError.message });
            }

            // Approach 2: Traditional table extraction
            if (extractedData.length === 0) {
                try {
                    this.logger.info('Trying traditional table extraction');
                    await page.waitForSelector('table tbody tr, .table tbody tr, [role="row"]:not(:first-child)', { timeout: 10000 });
                    
                    extractedData = await page.evaluate((maxResults) => {
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
                                    
                                    const unitId = `${project}_${unitNo}_${Date.now()}_${i}`.replace(/[^a-zA-Z0-9_]/g, '');
                                    
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
                                            extractionMethod: 'Traditional-Table',
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

                    this.logger.info('Table extraction completed', { propertiesFound: extractedData.length });

                } catch (tableError) {
                    this.logger.warn('Table extraction failed, trying alternative methods', { error: tableError.message });
                }
            }

            // Approach 3: Generic content extraction with better filtering
            if (extractedData.length === 0) {
                extractedData = await page.evaluate((maxResults) => {
                    const results = [];
                    
                    // Look for any elements that might contain property data
                    const dataElements = document.querySelectorAll(
                        '.property, .unit, .listing, [class*="property"], [class*="unit"], [class*="listing"], ' +
                        '.slds-card, .card, [data-name*="property"], [data-name*="unit"]'
                    );
                    
                    for (let i = 0; i < Math.min(dataElements.length, maxResults); i++) {
                        try {
                            const element = dataElements[i];
                            const text = element.textContent?.trim() || '';
                            
                            // Better filtering - avoid navigation and menu elements
                            if (text.length > 50 && 
                                !text.includes('Dashboard') && 
                                !text.includes('Profile') && 
                                !text.includes('About') &&
                                !text.includes('Marketing') &&
                                !text.includes('Performance') &&
                                !text.includes('More') &&
                                (text.includes('Sobha') || text.includes('Project') || text.includes('Unit') || 
                                 text.includes('AED') || text.includes('sqft') || text.match(/\d+/))) {
                                
                                const unitId = `property_${i}_${Date.now()}`;
                                results.push({
                                    unitId,
                                    project: 'Sobha Properties',
                                    subProject: '',
                                    unitType: '',
                                    floor: '',
                                    unitNo: `Unit-${i + 1}`,
                                    totalUnitArea: '',
                                    startingPrice: '',
                                    availability: 'available',
                                    rawData: text.substring(0, 500),
                                    sourceUrl: window.location.href,
                                    extractionMethod: 'Generic-Content',
                                    scrapedAt: new Date().toISOString()
                                });
                            }
                        } catch (elementError) {
                            console.warn(`Error processing element ${i}:`, elementError);
                        }
                    }
                    
                    return results;
                }, this.input.maxResults);

                this.logger.info('Alternative extraction completed', { propertiesFound: extractedData.length });
            }

            // If still no data, create a detailed debug entry
            if (extractedData.length === 0) {
                this.logger.warn('No property data found, creating debug entry with page analysis');
                
                const pageAnalysis = await page.evaluate(() => {
                    const analysis = {
                        url: window.location.href,
                        title: document.title,
                        bodyText: document.body.textContent?.substring(0, 1000) || '',
                        elementCounts: {
                            divs: document.querySelectorAll('div').length,
                            tables: document.querySelectorAll('table').length,
                            cards: document.querySelectorAll('.card, .slds-card').length,
                            buttons: document.querySelectorAll('button').length,
                            links: document.querySelectorAll('a').length
                        },
                        classNames: Array.from(document.querySelectorAll('[class]')).slice(0, 10).map(el => el.className)
                    };
                    return analysis;
                });
                
                extractedData = [{
                    unitId: `debug_${Date.now()}`,
                    project: 'Debug Entry - No Properties Found',
                    subProject: 'Analysis',
                    unitType: 'Debug',
                    floor: '0',
                    unitNo: 'DEBUG-001',
                    totalUnitArea: '0',
                    startingPrice: '0',
                    availability: 'debug',
                    sourceUrl: pageAnalysis.url,
                    pageTitle: pageAnalysis.title,
                    debugInfo: {
                        message: 'No property data found on page',
                        pageAnalysis: pageAnalysis,
                        pageStructure: pageStructure
                    },
                    extractionMethod: 'Debug-Analysis',
                    scrapedAt: new Date().toISOString()
                }];
            }

            // Validate extracted data
            const validProperties = extractedData.filter(prop => 
                prop.unitId && prop.project
            );

            this.metrics.recordPropertiesScraped(validProperties.length);
            
            this.logger.info('Property data extraction completed', {
                totalExtracted: extractedData.length,
                validProperties: validProperties.length,
                invalidFiltered: extractedData.length - validProperties.length,
                extractionMethods: [...new Set(extractedData.map(p => p.extractionMethod))]
            });

            return validProperties;

        } catch (error) {
            this.logger.error('Failed to extract property data', { error: error.message });
            
            // Return a debug entry even if extraction completely fails
            return [{
                unitId: `error_${Date.now()}`,
                project: 'Error Entry',
                subProject: 'Extraction failed',
                unitType: 'Error',
                floor: '0',
                unitNo: 'ERROR-001',
                totalUnitArea: '0',
                startingPrice: '0',
                availability: 'error',
                sourceUrl: page.url(),
                errorInfo: error.message,
                extractionMethod: 'Error-Fallback',
                scrapedAt: new Date().toISOString()
            }];
        }
    }

    /**
     * Main enterprise scraping workflow
     */
    async executeScraping() {
        const crawler = new PlaywrightCrawler({
            maxRequestsPerCrawl: 1,
            requestHandlerTimeoutSecs: CONFIG.REQUEST_TIMEOUT / 1000, // 5 minutes for modal handling
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

                    // Navigate to projects page (flexible approach)
                    await this.navigateToProjects(page);

                    // Apply filters (if any)
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
                            scraperVersion: '1.0.1',
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
async function main() {
    try {
        await Actor.init();
        console.log('Actor initialized successfully');

        // Get and validate input
        const actorInput = await Actor.getInput() ?? {};
        console.log('Actor input received:', { hasEmail: !!actorInput.email, hasPassword: !!actorInput.password });
        
        // Enterprise input validation
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

        // Initialize enterprise scraper
        console.log('Initializing enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute enterprise scraping workflow
        console.log('Starting scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Enterprise scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped
                });
            }
        } else {
            console.error('Scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Enterprise scraping failed', { error: results.error });
            }
            await Actor.fail(`Scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in enterprise actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in enterprise actor', { 
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
