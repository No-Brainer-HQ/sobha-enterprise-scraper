/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.5 - FIXED LIGHTNING TABLE EXTRACTION
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { randomBytes, createHash } from 'crypto';
import { performance } from 'perf_hooks';

/**
 * Enterprise Configuration Constants - OPTIMIZED FOR LIGHTNING TABLE EXTRACTION
 */
const CONFIG = {
    // Performance settings
    MAX_CONCURRENT_REQUESTS: 2,
    REQUEST_TIMEOUT: 1800000, // 30 minutes for large datasets
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
    
    // Selectors - UPDATED FOR LIGHTNING TABLE EXTRACTION
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]',
        
        // Lightning table selectors
        filterPropertiesButton: 'button:has-text("Filter Properties")',
        propertyModal: '[role="dialog"], .slds-modal',
        lightningTable: 'tbody[lwc-774enseH4rp], tbody',
        lightningTableRows: 'tr.slds-hint-parent, tr[lwc-774enseH4rp], tr',
        lightningTableCells: 'td',
        
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
 * Enterprise Sobha Portal Scraper - WITH LIGHTNING TABLE EXTRACTION
 */
class EnterpriseSobhaPortalScraper {
    constructor(validatedInput) {
        this.input = validatedInput;
        this.sessionId = SecurityManager.generateSessionId();
        this.logger = new EnterpriseLogger(this.sessionId);
        this.rateLimiter = new RateLimiter(this.input.requestDelay * 1000);
        this.metrics = new MetricsCollector(this.sessionId);
        
        this.logger.info('Lightning table-aware enterprise scraper initialized', {
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

                    // Try to dismiss any modal (enhanced for promotional modal)
                    await this.dismissPostLoginModal(page);
                    
                    // Extended wait for modal dismissal to complete
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
     * ENHANCED: Modal dismissal targeting promotional modal specifically
     */
    async dismissPostLoginModal(page) {
        try {
            this.logger.info('Attempting to dismiss post-login promotional modal');

            // Wait for promotional modal to appear
            await page.waitForTimeout(3000);

            // STEP 1: Target the specific promotional modal (6% commission modal)
            this.logger.info('Looking for promotional modal (6% commission)');
            
            // Enhanced promotional modal selectors (targeting specific Lightning component)
            const promotionalModalSelectors = [
                // Target the specific Lightning component modal
                '[c-brokerportalhomepage_brokerportalhomepage] button:has-text("×")',
                '[c-brokerportalhomepage_brokerportalhomepage] .slds-modal button:has-text("×")',
                '[c-brokerportalhomepage_brokerportalhomepage] button[aria-label*="close"]',
                '[c-brokerportalhomepage_brokerportalhomepage] button[title*="close"]',
                
                // Target the full modal structure close buttons
                '.slds-modal.slds-fade-in-open.slds-modal_full button:has-text("×")',
                '.slds-modal.slds-fade-in-open.slds-modal_full button[aria-label*="close"]',
                '.slds-modal.slds-fade-in-open.slds-modal_full .slds-modal__close',
                
                // Generic Lightning component close buttons
                '[c-brokerportalhomepage_brokerportalhomepage] .slds-button_icon',
                '[c-brokerportalhomepage_brokerportalhomepage] .slds-button_icon-inverse',
                '[c-brokerportalhomepage_brokerportalhomepage] [data-key="close"]',
                
                // Fallback to standard modal selectors
                'button:has-text("×")',
                '[role="dialog"] button:has-text("×")',
                '.slds-modal button:has-text("×")',
                'button[aria-label*="close"]',
                'button[title*="close"]',
                '.slds-modal__close'
            ];

            let modalClosed = false;
            
            for (const selector of promotionalModalSelectors) {
                try {
                    this.logger.debug(`Trying promotional modal selector: ${selector}`);
                    
                    await page.waitForSelector(selector, { timeout: 3000, state: 'visible' });
                    this.logger.info(`Found promotional modal close button: ${selector}`);
                    
                    await page.click(selector);
                    await page.waitForTimeout(2000);
                    
                    // Verify modal is closed by checking if button is still visible
                    const stillVisible = await page.isVisible(selector).catch(() => false);
                    if (!stillVisible) {
                        this.logger.info('✅ Promotional modal closed successfully');
                        modalClosed = true;
                        break;
                    }
                    
                } catch (error) {
                    this.logger.debug(`Promotional modal selector failed: ${selector}`, { error: error.message });
                }
            }

            // STEP 2: If promotional modal selectors didn't work, try JavaScript approach
            if (!modalClosed) {
                this.logger.info('Trying JavaScript approach to close promotional modal');
                
                try {
                    const modalClosedByJS = await page.evaluate(() => {
                        console.log('Looking for promotional modal via JavaScript...');
                        
                        // First, look for the specific Lightning filter component
                        const lightningFilterComponent = document.querySelector('[c-brokerportalsohbaprojectfilter_brokerportalsohbaprojectfilter]');
                        if (lightningFilterComponent) {
                            console.log('Found Lightning filter component');
                            
                            // Look for clickable elements within the Lightning filter component
                            const clickables = lightningFilterComponent.querySelectorAll('button, a, [role="button"]');
                            console.log(`Lightning filter component has ${clickables.length} clickable elements`);
                            
                            for (const element of clickables) {
                                const text = element.textContent || element.innerHTML || '';
                                const dataElement = element.getAttribute('data-element') || '';
                                const className = element.className || '';
                                
                                // Check if it's the filter button
                                if (text.toLowerCase().includes('filter') || 
                                    text.toLowerCase().includes('properties') ||
                                    dataElement === 'general-enquiry' ||
                                    className.includes('btn')) {
                                    
                                    console.log(`Found Lightning filter element: ${text || dataElement || 'btn element'}`);
                                    element.click();
                                    return true;
                                }
                            }
                        }
                        
                        // Fallback: Look for all clickable elements (including <a> tags)
                        console.log('Looking for clickable elements including <a> tags...');
                        const allElements = Array.from(document.querySelectorAll(
                            'button, a, input[type="button"], input[type="submit"], [role="button"], [onclick], lightning-button'
                        ));
                        
                        console.log(`Found ${allElements.length} potentially clickable elements`);
                        
                        for (const element of allElements) {
                            const text = (element.textContent || element.value || '').toLowerCase();
                            const ariaLabel = (element.getAttribute('aria-label') || '').toLowerCase();
                            const className = (element.className || '').toLowerCase();
                            const id = (element.id || '').toLowerCase();
                            const dataElement = (element.getAttribute('data-element') || '').toLowerCase();
                            const tagName = element.tagName.toLowerCase();
                            
                            // Look for filter-related keywords with <a> tag priority
                            const searchTerms = ['filter', 'search', 'apply', 'properties', 'submit'];
                            const hasFilterKeyword = searchTerms.some(term => 
                                text.includes(term) || ariaLabel.includes(term) || className.includes(term) || id.includes(term)
                            );
                            
                            // Special check for the exact element from HTML inspection
                            const isFilterPropertiesLink = (
                                tagName === 'a' && 
                                text.includes('filter') && 
                                text.includes('properties') &&
                                (className.includes('btn') || dataElement === 'general-enquiry')
                            );
                            
                            if ((hasFilterKeyword || isFilterPropertiesLink) && element.offsetParent !== null) { // Visible element
                                console.log(`Found potential filter element: "${text || ariaLabel || className}" - Tag: ${tagName} - attempting click`);
                                
                                try {
                                    element.click();
                                    console.log('Element clicked successfully');
                                    return true;
                                } catch (clickError) {
                                    console.log(`Click failed: ${clickError}`);
                                }
                            }
                        }
                        
                        console.log('No close button found via JavaScript');
                        return false;
                    });
                    
                    if (modalClosedByJS) {
                        this.logger.info('✅ Promotional modal closed via JavaScript');
                        modalClosed = true;
                        await page.waitForTimeout(2000);
                    }
                } catch (jsError) {
                    this.logger.debug('JavaScript modal close failed', { error: jsError.message });
                }
            }

            // STEP 3: Force close with escape key and click outside
            if (!modalClosed) {
                this.logger.info('Trying escape key and backdrop click to close modal');
                
                try {
                    // Multiple escape presses
                    await page.keyboard.press('Escape');
                    await page.waitForTimeout(500);
                    await page.keyboard.press('Escape');
                    await page.waitForTimeout(500);
                    
                    // Click on backdrop/outside area
                    await page.click('body', { position: { x: 10, y: 10 } });
                    await page.waitForTimeout(1000);
                    
                    this.logger.info('Escape key and backdrop click completed');
                } catch (escapeError) {
                    this.logger.debug('Escape key approach failed', { error: escapeError.message });
                }
            }

            // STEP 4: Verify modal dismissal worked
            await page.waitForTimeout(2000);
            
            const finalModalCount = await page.evaluate(() => {
                // Check for both general modals and the specific Lightning component modal
                const generalModals = Array.from(document.querySelectorAll('[role="dialog"], .slds-modal'))
                    .filter(modal => modal.offsetParent !== null);
                    
                const lightningComponentModals = Array.from(document.querySelectorAll('[c-brokerportalhomepage_brokerportalhomepage]'))
                    .filter(modal => modal.offsetParent !== null && modal.querySelector('.slds-modal'));
                    
                return generalModals.length + lightningComponentModals.length;
            });
            
            if (finalModalCount === 0) {
                this.logger.info('✅ All modals successfully dismissed');
            } else {
                this.logger.warn(`${finalModalCount} modal(s) still visible after dismissal attempts`);
            }

            this.logger.info('Enhanced modal dismissal completed');

        } catch (error) {
            this.logger.warn('Modal dismissal failed but continuing', { error: error.message });
        }
    }

    /**
     * Navigate to projects page and wait for Lightning components to render
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
                
                // CRITICAL: Wait for Lightning components to render
                this.logger.info('Waiting for Lightning components to render');
                await this.waitForLightningComponentsToRender(page);
                
                this.logger.info('✅ Direct navigation and Lightning rendering completed');
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
     * NEW: Wait for Lightning components to fully render
     */
    async waitForLightningComponentsToRender(page) {
        try {
            this.logger.info('Waiting for Lightning components to fully render');

            // Step 1: Wait for the main Lightning component to appear
            this.logger.debug('Waiting for main Lightning component');
            await page.waitForFunction(() => {
                // Wait for the specific Sobha projects component
                const sobhaComponent = document.querySelector('c-brokerportalsohbaprojects, [class*="brokerportalsohbaprojects"]');
                if (sobhaComponent) {
                    console.log('Sobha Lightning component found');
                    return true;
                }
                
                // Also check for general Lightning content
                const lightningContent = document.querySelectorAll('[class*="slds-"], [data-aura-rendered-by]');
                console.log(`Found ${lightningContent.length} Lightning elements`);
                return lightningContent.length > 50; // Substantial Lightning content
            }, {}, { timeout: 30000 });

            // Step 2: Wait for UI elements to be rendered inside the component
            this.logger.debug('Waiting for UI content to render inside Lightning components');
            await page.waitForFunction(() => {
                // Check for any buttons or interactive elements
                const buttons = document.querySelectorAll('button');
                const inputs = document.querySelectorAll('input');
                const clickables = document.querySelectorAll('[onclick], [role="button"]');
                
                const totalInteractive = buttons.length + inputs.length + clickables.length;
                console.log(`Found ${totalInteractive} interactive elements`);
                
                // Also check for specific filter-related content
                const bodyText = document.body.textContent || '';
                const hasFilterContent = bodyText.includes('Filter') || bodyText.includes('Properties') || bodyText.includes('Search');
                
                console.log(`Has filter content: ${hasFilterContent}`);
                console.log(`Total interactive elements: ${totalInteractive}`);
                
                return totalInteractive >= 5 && hasFilterContent;
            }, {}, { timeout: 45000 });

            // Step 3: Additional wait for any final rendering
            this.logger.debug('Allowing extra time for final component rendering');
            await page.waitForTimeout(5000);

            // Step 4: Verify components are ready
            const componentStatus = await page.evaluate(() => {
                const sobhaComponent = document.querySelector('c-brokerportalsohbaprojects, [class*="brokerportalsohbaprojects"]');
                const buttons = document.querySelectorAll('button');
                const bodyText = document.body.textContent || '';
                
                return {
                    hasSobhaComponent: !!sobhaComponent,
                    buttonCount: buttons.length,
                    hasFilterText: bodyText.includes('Filter'),
                    hasPropertiesText: bodyText.includes('Properties'),
                    contentLength: bodyText.length
                };
            });

            this.logger.info('Lightning component rendering completed', componentStatus);

            if (componentStatus.buttonCount === 0) {
                throw new Error('No buttons found after Lightning rendering - components may not have loaded properly');
            }

            return true;

        } catch (error) {
            this.logger.error('Lightning component rendering failed', { error: error.message });
            
            // Don't fail completely - log the issue but continue
            this.logger.warn('Continuing despite Lightning rendering issues');
            await page.waitForTimeout(10000); // Extra wait as fallback
            return false;
        }
    }

/**
 * ALTERNATIVE SOLUTION - Waits for actual content instead of counting cells
 */
async openPropertyModal(page) {
    try {
        this.logger.info('Opening property modal - Alternative approach');

        await page.waitForTimeout(3000);

        // Click Filter Properties
        this.logger.info('Clicking Filter Properties button');
        await page.click('a:has-text("Filter Properties")');
        this.logger.info('✅ Filter Properties clicked');

        // Wait for modal structure
        await page.waitForTimeout(5000);

        // Wait for spinner to disappear using Playwright's method
        this.logger.info('Checking for spinner...');
        const spinnerSelector = 'lightning-spinner';
        
        try {
            // First check if spinner exists
            const spinnerExists = await page.isVisible(spinnerSelector);
            
            if (spinnerExists) {
                this.logger.info('Spinner detected, waiting for it to disappear (up to 40 seconds)...');
                await page.waitForSelector(spinnerSelector, { 
                    state: 'hidden', 
                    timeout: 40000 
                });
                this.logger.info('✅ Spinner disappeared');
            } else {
                this.logger.info('No spinner detected');
            }
        } catch (e) {
            this.logger.warn('Spinner handling error, continuing anyway');
        }

        // Additional wait for data to render after spinner
        this.logger.info('Waiting 10 seconds for data to render after spinner...');
        await page.waitForTimeout(10000);

        // Wait for table rows with actual content (not empty rows)
        this.logger.info('Waiting for table rows with content...');
        
        try {
            // Wait for at least one row with actual text content
            await page.waitForFunction(() => {
                const rows = document.querySelectorAll('table tbody tr');
                if (rows.length === 0) return false;
                
                // Check if any row has meaningful content
                for (const row of rows) {
                    const text = row.textContent?.trim() || '';
                    // Check if row has substantial text (not just whitespace or very short)
                    if (text.length > 20) {
                        console.log('Found row with content:', text.substring(0, 50));
                        return true;
                    }
                }
                return false;
            }, { timeout: 20000 });
            
            this.logger.info('✅ Found table rows with content');
        } catch (e) {
            this.logger.warn('Timeout waiting for content, checking current state...');
        }

        // Final verification - just check if we have ANY rows with text
        const tableStatus = await page.evaluate(() => {
            const tables = document.querySelectorAll('table');
            const results = [];
            
            tables.forEach((table, idx) => {
                const rows = table.querySelectorAll('tbody tr');
                let contentRows = 0;
                
                rows.forEach(row => {
                    const text = row.textContent?.trim() || '';
                    if (text.length > 10) { // Any row with more than 10 characters
                        contentRows++;
                    }
                });
                
                if (contentRows > 0) {
                    results.push({
                        tableIndex: idx,
                        totalRows: rows.length,
                        rowsWithContent: contentRows
                    });
                }
            });
            
            return results;
        });

        this.logger.info('Table status:', tableStatus);

        if (tableStatus.length === 0) {
            throw new Error('No tables with content found after waiting');
        }

        this.logger.info(`✅ Modal opened with ${tableStatus[0].rowsWithContent} rows of data`);
        return true;

    } catch (error) {
        this.logger.error('Failed to open property modal', { error: error.message });
        await page.screenshot({ path: './modal_error_state.png', fullPage: false });
        throw error;
    }
}

/**
 * Simple extraction that doesn't validate cell count
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Extracting property data - flexible approach');

        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Get ALL tables and try to extract from any with rows
            const tables = document.querySelectorAll('table');
            
            for (const table of tables) {
                const tbody = table.querySelector('tbody');
                if (!tbody) continue;
                
                const rows = tbody.querySelectorAll('tr');
                if (rows.length === 0) continue;
                
                console.log(`Found table with ${rows.length} rows, attempting extraction...`);
                
                rows.forEach((row, index) => {
                    try {
                        const cells = row.querySelectorAll('td');
                        
                        // Don't validate cell count - just extract what's there
                        if (cells.length > 0) {
                            const cellTexts = Array.from(cells).map(cell => 
                                cell.textContent?.trim() || ''
                            );
                            
                            // Only process if row has actual content
                            const hasContent = cellTexts.some(text => text.length > 0);
                            
                            if (hasContent) {
                                const property = {
                                    rowIndex: index + 1,
                                    cellCount: cells.length,
                                    // Try to map to expected fields (adjust indices if needed)
                                    project: cellTexts[0] || '',
                                    subProject: cellTexts[1] || '',
                                    unitType: cellTexts[2] || '',
                                    floor: cellTexts[3] || '',
                                    unitNo: cellTexts[4] || '',
                                    totalUnitArea: cellTexts[5] || '',
                                    startingPrice: cellTexts[6] || '',
                                    
                                    // Store all raw data for debugging
                                    rawData: cellTexts
                                };
                                
                                extractedProperties.push(property);
                                
                                // Log first few for debugging
                                if (index < 3) {
                                    console.log(`Row ${index + 1} (${cells.length} cells):`, cellTexts.join(' | '));
                                }
                            }
                        }
                    } catch (rowError) {
                        console.error(`Error in row ${index}:`, rowError.message);
                    }
                });
                
                // If we got data from this table, stop
                if (extractedProperties.length > 0) {
                    console.log(`Extracted ${extractedProperties.length} properties from table`);
                    break;
                }
            }
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        
        if (properties.length > 0) {
            // Log detailed info about what we extracted
            this.logger.info('Extraction summary:', {
                total: properties.length,
                firstProperty: properties[0],
                cellCounts: properties.slice(0, 5).map(p => p.cellCount)
            });
        }

        this.metrics.recordPropertiesScraped(properties.length);
        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
        return [];
    }
}
/**
 * Extract property data - matching the structure from screenshot
 * Columns: Project, Sub Project, Unit Type, Floor, Unit No., Total Unit Area, Starting Price
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Extracting property data from modal');

        // Give table a moment to stabilize
        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Find all tables and process the one with property data
            const tables = document.querySelectorAll('table');
            
            for (const table of tables) {
                const tbody = table.querySelector('tbody');
                if (!tbody) continue;
                
                const rows = tbody.querySelectorAll('tr');
                if (rows.length === 0) continue;
                
                // Check if this is the property table (has 7+ columns)
                const firstRow = rows[0];
                const testCells = firstRow.querySelectorAll('td');
                if (testCells.length < 7) continue;
                
                console.log(`Processing property table with ${rows.length} rows`);
                
                rows.forEach((row, index) => {
                    try {
                        const cells = row.querySelectorAll('td');
                        
                        if (cells.length >= 7) {
                            const getCellText = (cell) => {
                                // Remove any extra whitespace and return clean text
                                return cell.textContent?.trim() || '';
                            };
                            
                            const property = {
                                rowIndex: index + 1,
                                // Based on the screenshot columns:
                                project: getCellText(cells[0]),        // e.g., "Sobha Hartland"
                                subProject: getCellText(cells[1]),     // e.g., "Creek Vista"
                                unitType: getCellText(cells[2]),       // e.g., "Type A"
                                floor: getCellText(cells[3]),          // e.g., "18"
                                unitNo: getCellText(cells[4]),         // e.g., "A-1813"
                                totalUnitArea: getCellText(cells[5]),  // e.g., "788.46"
                                startingPrice: getCellText(cells[6]),  // e.g., "1,360,434"
                                
                                // Parse numeric values
                                floorNumber: parseInt(getCellText(cells[3])) || null,
                                area: parseFloat(getCellText(cells[5])?.replace(/,/g, '')) || null,
                                price: parseFloat(getCellText(cells[6])?.replace(/,/g, '')) || null
                            };
                            
                            // Validate we have meaningful data
                            if (property.unitNo && property.project) {
                                extractedProperties.push(property);
                                
                                // Log first few for verification
                                if (index < 3) {
                                    console.log(`Property ${index + 1}: ${property.project} - ${property.unitNo} - ${property.startingPrice}`);
                                }
                            }
                        }
                    } catch (rowError) {
                        console.error(`Error extracting row ${index}:`, rowError.message);
                    }
                });
                
                // If we found properties, stop looking
                if (extractedProperties.length > 0) break;
            }
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        
        if (properties.length > 0) {
            // Log sample data
            this.logger.info('Sample extracted properties:', {
                total: properties.length,
                first: properties[0],
                second: properties[1] || null
            });
        }

        this.metrics.recordPropertiesScraped(properties.length);
        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
        return [];
    }
}
/**
 * Alternative extraction method for Lightning Web Components
 * This handles dynamic data that might not have consistent IDs
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Extracting property data from Lightning components');

        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Find ANY table with data
            const tables = document.querySelectorAll('table');
            
            for (const table of tables) {
                const tbody = table.querySelector('tbody');
                if (!tbody) continue;
                
                const rows = tbody.querySelectorAll('tr');
                if (rows.length === 0) continue;
                
                console.log(`Processing table with ${rows.length} rows`);
                
                rows.forEach((row, index) => {
                    try {
                        const cells = row.querySelectorAll('td');
                        
                        // We need at least 6-7 cells for property data
                        if (cells.length >= 6) {
                            const getCellText = (cell) => {
                                // Try multiple methods to get text
                                
                                // Method 1: Look for slds-truncate div
                                const truncateDiv = cell.querySelector('div.slds-truncate, .slds-truncate');
                                if (truncateDiv) {
                                    return truncateDiv.getAttribute('title') || 
                                           truncateDiv.textContent?.trim() || '';
                                }
                                
                                // Method 2: Direct text content
                                return cell.textContent?.trim() || '';
                            };
                            
                            // Build property object based on cell position
                            const property = {
                                rowIndex: index + 1,
                                // Map cells to expected fields
                                projectCategory: getCellText(cells[0]),
                                project: getCellText(cells[1]),
                                unitType: getCellText(cells[2]),
                                floor: getCellText(cells[3]),
                                unitNo: getCellText(cells[4]),
                                totalUnitArea: getCellText(cells[5]),
                                startingPrice: cells[6] ? getCellText(cells[6]) : '',
                                
                                // Try to extract any additional data
                                rawData: Array.from(cells).map(c => getCellText(c))
                            };
                            
                            // Validate that we have meaningful data
                            const hasData = property.rawData.some(text => 
                                text && text.length > 0 && text !== '-'
                            );
                            
                            if (hasData) {
                                extractedProperties.push(property);
                                
                                // Log first few for debugging
                                if (index < 3) {
                                    console.log(`Row ${index}:`, property.rawData);
                                }
                            }
                        }
                    } catch (rowError) {
                        console.error(`Error extracting row ${index}:`, rowError.message);
                    }
                });
                
                // If we found data in this table, stop looking
                if (extractedProperties.length > 0) {
                    console.log(`Extracted ${extractedProperties.length} properties from table`);
                    break;
                }
            }
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        
        if (properties.length > 0) {
            this.logger.info('Sample property data:', {
                first: properties[0],
                total: properties.length
            });
        }

        this.metrics.recordPropertiesScraped(properties.length);

        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
        return [];
    }
}
/**
 * Debug version of extraction with detailed logging
 * Use this temporarily to diagnose what's happening
 */
async extractPropertyDataDebug(page) {
    try {
        this.logger.info('🔍 DEBUG: Starting detailed extraction diagnostics');

        await page.waitForTimeout(3000);

        // First, let's check what we can see in the DOM
        const domInfo = await page.evaluate(() => {
            const info = {
                hasModal: false,
                modalId: null,
                hasTbody: false,
                tbodySelectors: [],
                rowCount: 0,
                rowSelectors: [],
                firstRowInfo: null,
                sampleCellData: []
            };

            // Check for modal
            const modal = document.querySelector('[id^="modal-content-id-"]');
            info.hasModal = !!modal;
            info.modalId = modal?.id || 'not found';

            // Try different tbody selectors
            const tbodySelectors = [
                'tbody[lwc-774enseH4rp]',
                'tbody[lwc-774enseH4rp=""]', 
                'table.customFilterTable tbody',
                '.customFilterTable tbody',
                '[id^="modal-content-id-"] tbody',
                'tbody'
            ];

            for (const selector of tbodySelectors) {
                const tbody = document.querySelector(selector);
                if (tbody) {
                    info.tbodySelectors.push(selector);
                }
            }

            // Find the actual tbody
            const tbody = document.querySelector('[id^="modal-content-id-"] tbody') || 
                         document.querySelector('tbody');
            info.hasTbody = !!tbody;

            if (tbody) {
                // Try different row selectors
                const rowSelectors = [
                    'tr.slds-hint-parent',
                    'tr[lwc-774enseH4rp]',
                    'tr[lwc-774enseH4rp=""]',
                    'tr'
                ];

                for (const selector of rowSelectors) {
                    const rows = tbody.querySelectorAll(selector);
                    if (rows.length > 0) {
                        info.rowSelectors.push(`${selector}: ${rows.length} rows`);
                    }
                }

                // Get all rows
                const rows = tbody.querySelectorAll('tr');
                info.rowCount = rows.length;

                // Examine first row in detail
                if (rows.length > 0) {
                    const firstRow = rows[0];
                    const cells = firstRow.querySelectorAll('td');
                    
                    info.firstRowInfo = {
                        cellCount: cells.length,
                        rowClasses: firstRow.className,
                        rowAttributes: Array.from(firstRow.attributes).map(attr => 
                            `${attr.name}="${attr.value}"`
                        ).join(' ')
                    };

                    // Get data from first 3 cells as sample
                    for (let i = 0; i < Math.min(3, cells.length); i++) {
                        const cell = cells[i];
                        const truncateDiv = cell.querySelector('div.slds-truncate');
                        
                        info.sampleCellData.push({
                            cellIndex: i,
                            dataLabel: cell.getAttribute('data-label'),
                            hasTruncateDiv: !!truncateDiv,
                            titleAttr: truncateDiv?.getAttribute('title'),
                            textContent: truncateDiv?.textContent?.trim() || cell.textContent?.trim(),
                            innerHTML: cell.innerHTML.substring(0, 200) // First 200 chars
                        });
                    }
                }
            }

            return info;
        });

        this.logger.info('🔍 DEBUG DOM Info:', domInfo);

        // Now try extraction with detailed logging
        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            const debugLog = [];

            try {
                // Find modal
                const modal = document.querySelector('[id^="modal-content-id-"]');
                if (!modal) {
                    debugLog.push('ERROR: Modal not found');
                    return { properties: [], debugLog };
                }
                debugLog.push(`Found modal: ${modal.id}`);

                // Find tbody
                const tbody = modal.querySelector('tbody') || 
                            document.querySelector('[id^="modal-content-id-"] tbody');
                if (!tbody) {
                    debugLog.push('ERROR: Tbody not found');
                    return { properties: [], debugLog };
                }
                debugLog.push('Found tbody');

                // Get rows - try multiple selectors
                let rows = tbody.querySelectorAll('tr.slds-hint-parent');
                if (rows.length === 0) {
                    rows = tbody.querySelectorAll('tr[lwc-774enseH4rp]');
                    debugLog.push('Using tr[lwc-774enseH4rp] selector');
                }
                if (rows.length === 0) {
                    rows = tbody.querySelectorAll('tr');
                    debugLog.push('Using generic tr selector');
                }
                
                debugLog.push(`Found ${rows.length} rows`);

                // Process first 5 rows for debugging
                const maxRows = Math.min(5, rows.length);
                for (let i = 0; i < maxRows; i++) {
                    const row = rows[i];
                    const cells = row.querySelectorAll('td');
                    
                    debugLog.push(`Row ${i}: ${cells.length} cells`);

                    if (cells.length >= 7) {
                        const property = {
                            rowIndex: i + 1,
                            cells: []
                        };

                        // Extract each cell
                        for (let j = 0; j < Math.min(8, cells.length); j++) {
                            const cell = cells[j];
                            const truncateDiv = cell.querySelector('div.slds-truncate');
                            
                            let cellText = '';
                            if (truncateDiv) {
                                cellText = truncateDiv.getAttribute('title') || 
                                          truncateDiv.textContent?.trim() || '';
                            } else {
                                cellText = cell.textContent?.trim() || '';
                            }

                            property.cells.push({
                                index: j,
                                label: cell.getAttribute('data-label'),
                                text: cellText
                            });
                        }

                        // Map to expected structure
                        property.projectCategory = property.cells[0]?.text || '';
                        property.project = property.cells[1]?.text || '';
                        property.unitType = property.cells[2]?.text || '';
                        property.floor = property.cells[3]?.text || '';
                        property.unitNo = property.cells[4]?.text || '';
                        property.totalUnitArea = property.cells[5]?.text || '';
                        property.startingPrice = property.cells[6]?.text || '';

                        extractedProperties.push(property);
                        debugLog.push(`Extracted property ${i + 1}: Unit ${property.unitNo}`);
                    }
                }

            } catch (error) {
                debugLog.push(`ERROR: ${error.message}`);
            }

            return { 
                properties: extractedProperties, 
                debugLog,
                totalRowsFound: document.querySelectorAll('[id^="modal-content-id-"] tbody tr').length
            };
        });

        this.logger.info('🔍 DEBUG Extraction Results:', {
            propertiesExtracted: properties.properties.length,
            totalRows: properties.totalRowsFound,
            debugLog: properties.debugLog,
            firstProperty: properties.properties[0]
        });

        return properties.properties;

    } catch (error) {
        this.logger.error('🔍 DEBUG: Extraction failed', { 
            error: error.message,
            stack: error.stack 
        });
        return [];
    }
}
    /**
     * Main enhanced scraping workflow with Lightning table extraction
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
                    this.logger.info('Starting Lightning table-aware scraping workflow', { url: request.url });

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

                    // Open property modal by clicking Filter Properties
                    await this.openPropertyModal(page);

                    // Extract property data from Lightning table modal
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
                            approach: 'lightning-table-extraction'
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
                            scraperVersion: '1.0.5',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            approach: 'lightning-table-extraction',
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

                    this.logger.info('Lightning table-aware scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart)
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Lightning table-aware scraping workflow failed', {
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
                        approach: 'lightning-table-extraction'
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
 * MAIN LIGHTNING TABLE-AWARE ACTOR ENTRY POINT
 */
async function main() {
    try {
        await Actor.init();
        console.log('Lightning table-aware Actor initialized successfully');

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

        // Initialize Lightning table-aware scraper
        console.log('Initializing Lightning table-aware enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute Lightning table-aware scraping workflow
        console.log('Starting Lightning table-aware scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Lightning table-aware scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Lightning table-aware scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped
                });
            }
        } else {
            console.error('Lightning table-aware scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Lightning table-aware scraping failed', { error: results.error });
            }
            await Actor.fail(`Lightning table-aware scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in Lightning table-aware actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in Lightning table-aware actor', { 
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
