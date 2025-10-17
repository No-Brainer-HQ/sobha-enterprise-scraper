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
 * WORKING VERSION: Using exact selectors from live website
 */
async openPropertyModal(page) {
    try {
        this.logger.info('Opening property listings modal');

        // Wait for page to stabilize
        await page.waitForTimeout(3000);

        // Find and click the "Filter Properties" button
        this.logger.info('Looking for Filter Properties button');
        
        const lightningFilterSelectors = [
            'a[data-element="general-enquiry"]:has-text("Filter Properties")',
            'a.btn:has-text("Filter Properties")',
            'a:has-text("Filter Properties")',
            'button:has-text("Filter Properties")',
        ];

        let buttonFound = false;
        for (const selector of lightningFilterSelectors) {
            try {
                await page.waitForSelector(selector, { timeout: 5000, state: 'visible' });
                this.logger.info(`Found button: ${selector}`);
                await page.click(selector);
                buttonFound = true;
                break;
            } catch (error) {
                this.logger.debug(`Selector failed: ${selector}`);
            }
        }

        if (!buttonFound) {
            throw new Error('Filter Properties button not found');
        }

        // Wait for modal section to appear (using your exact structure)
        this.logger.info('Waiting for property modal to open (20 second timeout)');
        await page.waitForSelector('c-broker-portal-unit-filter-component section', { 
            timeout: 20000,  // Increased to 20 seconds
            state: 'attached'  // Changed to 'attached' instead of 'visible'
        });

        // Additional wait to ensure modal is fully rendered
        await page.waitForTimeout(2000);
        
        // Verify modal is actually there and interactive
        const modalReady = await page.evaluate(() => {
            const section = document.querySelector('c-broker-portal-unit-filter-component section');
            return section && section.offsetParent !== null;
        });
        
        if (!modalReady) {
            this.logger.warn('Modal section found but not interactive, waiting more...');
            await page.waitForTimeout(3000);
        }

        this.logger.info('Modal opened, checking for errors');

        // Check for CSS/Aura error
        const hasError = await page.evaluate(() => {
            const errorDialog = document.querySelector('#auraError, #auraErrorMessage');
            if (errorDialog && errorDialog.offsetParent !== null) {
                return errorDialog.textContent || 'Unknown error';
            }
            return null;
        });

        if (hasError && hasError.includes('CSS Error')) {
            this.logger.warn('CSS Error detected, reloading modal');
            // Click refresh on error dialog
            await page.click('#auraErrorReload').catch(() => {});
            await page.waitForTimeout(3000);
            // Try clicking filter button again
            await page.click('a[data-element="general-enquiry"]:has-text("Filter Properties")');
            await page.waitForTimeout(2000);
        }

        // Wait for YOUR EXACT spinner selector to disappear
        this.logger.info('Waiting for Lightning spinner to disappear...');
        const spinnerSelector = 'c-broker-portal-unit-filter-component lightning-spinner';
        
        let spinnerGone = false;
        let attempts = 0;
        const maxAttempts = 30;
        
        while (!spinnerGone && attempts < maxAttempts) {
            attempts++;
            
            spinnerGone = await page.evaluate((selector) => {
                const spinner = document.querySelector(selector);
                return !spinner || spinner.offsetParent === null;
            }, spinnerSelector);
            
            if (spinnerGone) {
                this.logger.info(`✅ Spinner disappeared after ${attempts * 2} seconds`);
                break;
            }
            
            this.logger.debug(`Waiting for spinner... (${attempts * 2}s elapsed)`);
            await page.waitForTimeout(2000);
        }

        // Extra wait for content to render
        await page.waitForTimeout(3000);

        // Wait for table with dynamic modal ID
        this.logger.info('Waiting for table to load...');
        
        // The modal has dynamic ID, so we look for pattern
        await page.waitForSelector('[id^="modal-content-id-"] table tbody', {
            timeout: 30000,
            state: 'visible'
        });

        // Debug - see what we found
        const tableInfo = await page.evaluate(() => {
            // Find modal content div with dynamic ID
            const modalContent = document.querySelector('[id^="modal-content-id-"]');
            const table = modalContent ? modalContent.querySelector('table') : null;
            const tbody = table ? table.querySelector('tbody') : null;
            const rows = tbody ? tbody.querySelectorAll('tr') : [];
            
            return {
                hasModalContent: !!modalContent,
                modalId: modalContent?.id || 'not found',
                hasTable: !!table,
                hasTbody: !!tbody,
                rowCount: rows.length,
                firstRowCells: rows[0] ? rows[0].querySelectorAll('td').length : 0
            };
        });

        this.logger.info('Table detection results:', tableInfo);

        // Wait for rows to load
        this.logger.info('Waiting for property rows to load...');
        
        let rowsLoaded = false;
        for (let i = 0; i < 10; i++) {
            const rowCount = await page.evaluate(() => {
                const modalContent = document.querySelector('[id^="modal-content-id-"]');
                if (!modalContent) return 0;
                const tbody = modalContent.querySelector('table tbody');
                if (!tbody) return 0;
                return tbody.querySelectorAll('tr').length;
            });
            
            this.logger.info(`Check ${i + 1}/10: Found ${rowCount} rows`);
            
            if (rowCount > 0) {
                this.logger.info(`✅ Table loaded with ${rowCount} rows`);
                rowsLoaded = true;
                break;
            }
            
            await page.waitForTimeout(3000);
        }

        if (!rowsLoaded) {
            // Take screenshot for debugging
            await page.screenshot({ path: './debug_no_rows.png', fullPage: false });
            throw new Error('No rows loaded in table after waiting');
        }

        // Final verification
        const finalStatus = await page.evaluate(() => {
            const modalContent = document.querySelector('[id^="modal-content-id-"]');
            const tbody = modalContent ? modalContent.querySelector('table tbody') : null;
            const rows = tbody ? tbody.querySelectorAll('tr') : [];
            
            // Get sample data from first row
            let firstRowData = null;
            if (rows.length > 0) {
                const cells = rows[0].querySelectorAll('td');
                firstRowData = Array.from(cells).map(cell => cell.textContent?.trim() || '').slice(0, 8);
            }
            
            return {
                modalId: modalContent?.id || 'not found',
                rowCount: rows.length,
                cellsPerRow: rows[0] ? rows[0].querySelectorAll('td').length : 0,
                firstRowSample: firstRowData,
                hasSpinner: !!document.querySelector('lightning-spinner')
            };
        });

        this.logger.info('Final table status:', finalStatus);

        if (finalStatus.rowCount === 0) {
            throw new Error('No property rows found in table');
        }

        this.logger.info(`✅ Property modal opened successfully with ${finalStatus.rowCount} rows`);
        return true;

    } catch (error) {
        this.logger.error('Failed to open property modal', { 
            error: error.message,
            stack: error.stack 
        });
        
        // Take screenshot on error
        await page.screenshot({ path: './error_modal_state.png', fullPage: false });
        throw error;
    }
}

/**
 * Extract property data using exact selectors
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Starting property data extraction with exact selectors');

        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Find the modal content div with dynamic ID
            const modalContent = document.querySelector('[id^="modal-content-id-"]');
            if (!modalContent) {
                console.error('Modal content not found');
                return [];
            }
            
            // Get the tbody
            const tbody = modalContent.querySelector('table tbody');
            if (!tbody) {
                console.error('Table tbody not found');
                return [];
            }
            
            // Get all rows
            const rows = tbody.querySelectorAll('tr');
            console.log(`Found ${rows.length} rows to extract`);
            
            rows.forEach((row, index) => {
                try {
                    const cells = row.querySelectorAll('td');
                    
                    if (cells.length >= 7) {
                        const getCellText = (cell) => {
                            // Try to get text from div.slds-truncate first
                            const truncateDiv = cell.querySelector('div.slds-truncate');
                            if (truncateDiv) {
                                return truncateDiv.getAttribute('title') || truncateDiv.textContent?.trim() || '';
                            }
                            // Otherwise get direct text
                            return cell.textContent?.trim() || '';
                        };
                        
                        // Look for action button
                        const button = cells[cells.length - 1]?.querySelector('button') || 
                                     cells[7]?.querySelector('button');
                        
                        const property = {
                            rowIndex: index + 1,
                            projectCategory: getCellText(cells[0]),
                            project: getCellText(cells[1]),
                            unitType: getCellText(cells[2]),
                            floor: getCellText(cells[3]),
                            unitNo: getCellText(cells[4]),
                            totalUnitArea: getCellText(cells[5]),
                            startingPrice: getCellText(cells[6]),
                            recordId: button?.getAttribute('name') || button?.getAttribute('data-id') || '',
                            // Parsed values
                            floorNumber: parseInt(getCellText(cells[3])) || null,
                            area: parseFloat(getCellText(cells[5])?.replace(/[^0-9.]/g, '')) || null,
                            price: parseFloat(getCellText(cells[6])?.replace(/[^0-9.]/g, '')) || null
                        };
                        
                        // Only add if we have some data
                        if (property.unitNo || property.project) {
                            extractedProperties.push(property);
                        }
                    }
                } catch (rowError) {
                    console.error(`Error extracting row ${index}:`, rowError.message);
                }
            });
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        this.metrics.recordPropertiesScraped(properties.length);

        // Limit to maxResults if needed
        if (this.input.maxResults && properties.length > this.input.maxResults) {
            return properties.slice(0, this.input.maxResults);
        }

        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
        return [];
    }
}    

/**
 * Extract property data using exact selectors
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Starting property data extraction with exact selectors');

        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Find the modal content div with dynamic ID
            const modalContent = document.querySelector('[id^="modal-content-id-"]');
            if (!modalContent) {
                console.error('Modal content not found');
                return [];
            }
            
            // Get the tbody
            const tbody = modalContent.querySelector('table tbody');
            if (!tbody) {
                console.error('Table tbody not found');
                return [];
            }
            
            // Get all rows
            const rows = tbody.querySelectorAll('tr');
            console.log(`Found ${rows.length} rows to extract`);
            
            rows.forEach((row, index) => {
                try {
                    const cells = row.querySelectorAll('td');
                    
                    if (cells.length >= 7) {
                        const getCellText = (cell) => {
                            // Try to get text from div.slds-truncate first
                            const truncateDiv = cell.querySelector('div.slds-truncate');
                            if (truncateDiv) {
                                return truncateDiv.getAttribute('title') || truncateDiv.textContent?.trim() || '';
                            }
                            // Otherwise get direct text
                            return cell.textContent?.trim() || '';
                        };
                        
                        // Look for action button
                        const button = cells[cells.length - 1]?.querySelector('button') || 
                                     cells[7]?.querySelector('button');
                        
                        const property = {
                            rowIndex: index + 1,
                            projectCategory: getCellText(cells[0]),
                            project: getCellText(cells[1]),
                            unitType: getCellText(cells[2]),
                            floor: getCellText(cells[3]),
                            unitNo: getCellText(cells[4]),
                            totalUnitArea: getCellText(cells[5]),
                            startingPrice: getCellText(cells[6]),
                            recordId: button?.getAttribute('name') || button?.getAttribute('data-id') || '',
                            // Parsed values
                            floorNumber: parseInt(getCellText(cells[3])) || null,
                            area: parseFloat(getCellText(cells[5])?.replace(/[^0-9.]/g, '')) || null,
                            price: parseFloat(getCellText(cells[6])?.replace(/[^0-9.]/g, '')) || null
                        };
                        
                        // Only add if we have some data
                        if (property.unitNo || property.project) {
                            extractedProperties.push(property);
                        }
                    }
                } catch (rowError) {
                    console.error(`Error extracting row ${index}:`, rowError.message);
                }
            });
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        this.metrics.recordPropertiesScraped(properties.length);

        // Limit to maxResults if needed
        if (this.input.maxResults && properties.length > this.input.maxResults) {
            return properties.slice(0, this.input.maxResults);
        }

        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
        return [];
    }
}

/**
 * Extract property data - keep your original approach
 */
async extractPropertyData(page) {
    try {
        this.logger.info('Starting property data extraction');

        await page.waitForTimeout(2000);

        const properties = await page.evaluate(() => {
            const extractedProperties = [];
            
            // Try multiple selectors for rows
            let rows = document.querySelectorAll('tr.slds-hint-parent');
            if (rows.length === 0) {
                rows = document.querySelectorAll('.customFilterTable tbody tr');
            }
            if (rows.length === 0) {
                rows = document.querySelectorAll('tbody tr');
            }
            
            console.log(`Found ${rows.length} rows to extract`);
            
            rows.forEach((row, index) => {
                try {
                    const cells = row.querySelectorAll('td');
                    
                    if (cells.length >= 7) {
                        const getCellText = (cell) => {
                            const div = cell.querySelector('div.slds-truncate');
                            return div ? (div.getAttribute('title') || div.textContent?.trim() || '') : cell.textContent?.trim() || '';
                        };
                        
                        const button = cells[7]?.querySelector('button');
                        
                        const property = {
                            rowIndex: index + 1,
                            projectCategory: getCellText(cells[0]),
                            project: getCellText(cells[1]),
                            unitType: getCellText(cells[2]),
                            floor: getCellText(cells[3]),
                            unitNo: getCellText(cells[4]),
                            totalUnitArea: getCellText(cells[5]),
                            startingPrice: getCellText(cells[6]),
                            recordId: button?.getAttribute('name') || '',
                            floorNumber: parseInt(getCellText(cells[3])) || null,
                            area: parseFloat(getCellText(cells[5])) || null,
                            price: parseFloat(getCellText(cells[6])?.replace(/,/g, '')) || null
                        };
                        
                        if (property.unitNo && property.project) {
                            extractedProperties.push(property);
                        }
                    }
                } catch (rowError) {
                    console.error(`Error extracting row ${index}:`, rowError);
                }
            });
            
            return extractedProperties;
        });

        this.logger.info(`✅ Extracted ${properties.length} properties`);
        this.metrics.recordPropertiesScraped(properties.length);

        if (this.input.maxResults && properties.length > this.input.maxResults) {
            return properties.slice(0, this.input.maxResults);
        }

        return properties;

    } catch (error) {
        this.logger.error('Failed to extract property data', { error: error.message });
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
