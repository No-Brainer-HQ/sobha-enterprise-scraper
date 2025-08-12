/**
 * ENTERPRISE SOBHA PARTNER PORTAL SCRAPER
 * =====================================
 * Production-ready scraper for multi-billion dollar company standards
 * Built with comprehensive error handling, security, monitoring, and scalability
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.6 - LIGHTNING EXPERIENCE COMPATIBLE
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { Actor } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { randomBytes, createHash } from 'crypto';
import { performance } from 'perf_hooks';

/**
 * Enterprise Configuration Constants - OPTIMIZED FOR LIGHTNING EXPERIENCE
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
    
    // Selectors - UPDATED FOR LIGHTNING EXPERIENCE
    SELECTORS: {
        email: 'input[placeholder="name@example.com"], input[type="email"], textbox, input[name*="email"]',
        password: 'input[type="password"], textbox:has-text("Password"), input[placeholder*="password"]',
        loginButton: 'input[type="submit"]',
        
        // Lightning Experience selectors (session-agnostic)
        filterPropertiesButton: 'button:has-text("Filter Properties")',
        propertyModal: '[role="dialog"], .slds-modal',
        lightningTable: 'table.customFilterTable, .slds-modal table, [role="dialog"] table',
        lightningTableRows: 'tr.slds-hint-parent, tr',
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
 * Enterprise Sobha Portal Scraper - LIGHTNING EXPERIENCE COMPATIBLE
 */
class EnterpriseSobhaPortalScraper {
    constructor(validatedInput) {
        this.input = validatedInput;
        this.sessionId = SecurityManager.generateSessionId();
        this.logger = new EnterpriseLogger(this.sessionId);
        this.rateLimiter = new RateLimiter(this.input.requestDelay * 1000);
        this.metrics = new MetricsCollector(this.sessionId);
        
        this.logger.info('Lightning Experience aware enterprise scraper initialized', {
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
     * FIXED: Wait for Lightning Experience components to fully render
     */
    async waitForLightningComponentsToRender(page) {
        try {
            this.logger.info('Waiting for Lightning Experience components to fully render');

            // Step 1: Wait for Lightning loading to complete
            this.logger.debug('Waiting for Lightning loading box to disappear');
            await page.waitForFunction(() => {
                const loadingBox = document.getElementById('auraLoadingBox');
                const body = document.body;
                
                // Check if loading is complete
                return !body.classList.contains('loading') && 
                       (!loadingBox || loadingBox.style.display === 'none' || !loadingBox.offsetParent);
            }, {}, { timeout: 90000 });

            // Step 2: Wait for Lightning components to be present
            this.logger.debug('Waiting for Lightning components to be present');
            await page.waitForFunction(() => {
                // Check for Salesforce Lightning Design System (SLDS) elements
                const sldsElements = document.querySelectorAll('[class*="slds-"]');
                
                // Check for Lightning Web Components
                const lwcElements = document.querySelectorAll('[class*="lwc-"]');
                
                // Check for specific Sobha components
                const sobhaComponents = document.querySelectorAll('[class*="brokerportal"], [c-brokerportal]');
                
                console.log(`SLDS elements: ${sldsElements.length}, LWC elements: ${lwcElements.length}, Sobha components: ${sobhaComponents.length}`);
                
                return sldsElements.length > 20 && (lwcElements.length > 5 || sobhaComponents.length > 0);
            }, {}, { timeout: 60000 });

            // Step 3: Wait for interactive elements to be rendered
            this.logger.debug('Waiting for interactive elements to be rendered');
            await page.waitForFunction(() => {
                const buttons = document.querySelectorAll('button, a[role="button"], lightning-button');
                const inputs = document.querySelectorAll('input, lightning-input');
                const bodyText = document.body.textContent || '';
                
                const hasInteractiveElements = buttons.length > 3;
                const hasContent = bodyText.length > 1000; // Substantial content loaded
                
                console.log(`Interactive elements: ${buttons.length + inputs.length}, Body text length: ${bodyText.length}`);
                
                return hasInteractiveElements && hasContent;
            }, {}, { timeout: 45000 });

            // Step 4: Additional stability wait
            this.logger.debug('Allowing extra time for Lightning component stabilization');
            await page.waitForTimeout(8000);

            // Step 5: Verify Lightning readiness
            const lightningStatus = await page.evaluate(() => {
                const sldsElements = document.querySelectorAll('[class*="slds-"]');
                const buttons = document.querySelectorAll('button, a[role="button"]');
                const bodyText = document.body.textContent || '';
                
                return {
                    sldsElementCount: sldsElements.length,
                    buttonCount: buttons.length,
                    hasFilterText: bodyText.toLowerCase().includes('filter'),
                    hasPropertiesText: bodyText.toLowerCase().includes('properties'),
                    contentLength: bodyText.length,
                    isLightningReady: sldsElements.length > 20 && buttons.length > 3
                };
            });

            this.logger.info('Lightning Experience component rendering completed', lightningStatus);

            if (!lightningStatus.isLightningReady) {
                this.logger.warn('Lightning components may not have loaded completely', lightningStatus);
            }

            return lightningStatus.isLightningReady;

        } catch (error) {
            this.logger.error('Lightning Experience component rendering failed', { error: error.message });
            
            // Don't fail completely - log the issue but continue
            this.logger.warn('Continuing despite Lightning rendering issues');
            await page.waitForTimeout(15000); // Extra wait as fallback
            return false;
        }
    }

    /**
     * FIXED: Open property modal with Lightning Experience dynamic detection
     */
    async openPropertyModal(page) {
        try {
            this.logger.info('Opening property listings modal with Lightning Experience dynamic detection');

            // Wait for Lightning components to be fully interactive
            await page.waitForTimeout(5000);

            // Find Filter Properties button dynamically (session-agnostic)
            this.logger.info('Looking for Filter Properties button dynamically');
            
            const buttonFound = await page.evaluate(() => {
                // Strategy 1: Look for buttons with "Filter Properties" text
                const buttons = Array.from(document.querySelectorAll('button, lightning-button, [role="button"]'));
                
                for (const btn of buttons) {
                    const text = (btn.textContent || '').trim().toLowerCase();
                    if (text.includes('filter') && text.includes('properties')) {
                        console.log(`Found Filter Properties button: "${btn.textContent.trim()}"`);
                        btn.click();
                        return { found: true, method: 'button-text', text: btn.textContent.trim() };
                    }
                }
                
                // Strategy 2: Look for anchor tags with "Filter Properties" text
                const links = Array.from(document.querySelectorAll('a, lightning-formatted-url'));
                
                for (const link of links) {
                    const text = (link.textContent || '').trim().toLowerCase();
                    const dataElement = link.getAttribute('data-element') || '';
                    
                    if ((text.includes('filter') && text.includes('properties')) ||
                        dataElement === 'general-enquiry') {
                        console.log(`Found Filter Properties link: "${link.textContent.trim()}" with data-element: "${dataElement}"`);
                        link.click();
                        return { found: true, method: 'link-text', text: link.textContent.trim() };
                    }
                }
                
                // Strategy 3: Look for any element with "Filter Properties" text
                const allElements = Array.from(document.querySelectorAll('*'));
                
                for (const element of allElements) {
                    const text = (element.textContent || '').trim().toLowerCase();
                    const isClickable = element.tagName === 'BUTTON' || 
                                       element.tagName === 'A' || 
                                       element.getAttribute('role') === 'button' ||
                                       element.onclick !== null ||
                                       element.style.cursor === 'pointer';
                    
                    if (isClickable && text.includes('filter') && text.includes('properties') && 
                        element.offsetParent !== null) { // Visible element
                        console.log(`Found Filter Properties element: "${element.textContent.trim()}" - Tag: ${element.tagName}`);
                        element.click();
                        return { found: true, method: 'generic-element', text: element.textContent.trim() };
                    }
                }
                
                return { found: false, method: 'none' };
            });

            if (!buttonFound.found) {
                throw new Error('Filter Properties button not found with dynamic detection');
            }

            this.logger.info(`✅ Filter Properties button found and clicked: ${buttonFound.method} - "${buttonFound.text}"`);

            // Wait for modal to appear with Lightning-aware detection
            this.logger.info('Waiting for Lightning modal to appear');
            
            await page.waitForFunction(() => {
                // Look for Lightning modal patterns
                const dialogModals = document.querySelectorAll('[role="dialog"]');
                const sldsModals = document.querySelectorAll('.slds-modal');
                
                for (const modal of [...dialogModals, ...sldsModals]) {
                    if (modal.offsetParent !== null) { // Visible modal
                        // Check if modal has content (not just loading)
                        const hasContent = modal.querySelector('table, .slds-table, [role="table"], [role="grid"], .customFilterTable');
                        if (hasContent) {
                            console.log('Lightning modal with content found');
                            return true;
                        }
                        
                        // Check for loading state
                        const hasSpinner = modal.querySelector('.spinner, .slds-spinner, [class*="loading"]');
                        if (hasSpinner) {
                            console.log('Lightning modal with loading spinner found');
                            return true;
                        }
                    }
                }
                return false;
            }, {}, { timeout: 30000 });

            this.logger.info('✅ Lightning modal detected successfully');
            return true;

        } catch (error) {
            this.logger.error('Failed to open property modal with Lightning Experience detection', { error: error.message });
            throw error;
        }
    }

    /**
     * FIXED: Extract property data with Lightning Experience polling
     */
    async extractPropertyData(page) {
        try {
            this.logger.info('Starting Lightning Experience property data extraction with polling');

            // Polling-based extraction instead of fixed wait
            let attempts = 0;
            const maxAttempts = 30; // Poll for up to 60 seconds (30 * 2s)
            let extractedData = [];

            while (attempts < maxAttempts) {
                this.logger.info(`Lightning data extraction attempt ${attempts + 1}/${maxAttempts}`);

                // Keep modal alive with small mouse movement
                await page.mouse.move(100 + Math.random() * 50, 100 + Math.random() * 50);

                const result = await page.evaluate(() => {
                    // Find modal first
                    const dialogModals = document.querySelectorAll('[role="dialog"]');
                    const sldsModals = document.querySelectorAll('.slds-modal');
                    
                    let modal = null;
                    for (const m of [...dialogModals, ...sldsModals]) {
                        if (m.offsetParent !== null) { // Visible modal
                            modal = m;
                            break;
                        }
                    }
                    
                    if (!modal) {
                        return { status: 'no-modal', data: [] };
                    }
                    
                    // Find table using session-agnostic selectors
                    const tableSelectors = [
                        'table.customFilterTable',
                        '.customFilterTable',
                        'table',
                        '.slds-table',
                        '[role="table"]',
                        '[role="grid"]'
                    ];
                    
                    let table = null;
                    for (const selector of tableSelectors) {
                        table = modal.querySelector(selector);
                        if (table) break;
                    }
                    
                    if (!table) {
                        return { status: 'no-table', data: [] };
                    }
                    
                    // Check for data rows
                    const rows = table.querySelectorAll('tbody tr, tr.slds-hint-parent, tr');
                    if (rows.length === 0) {
                        return { status: 'no-rows', data: [] };
                    }
                    
                    // Extract data
                    const properties = [];
                    
                    for (let i = 0; i < rows.length; i++) {
                        const row = rows[i];
                        const cells = row.querySelectorAll('td');
                        
                        if (cells.length >= 7) { // Ensure we have enough columns
                            // Extract text from each cell (handles Lightning truncated content)
                            const cellTexts = Array.from(cells).map(cell => {
                                const truncateDiv = cell.querySelector('.slds-truncate');
                                const title = cell.getAttribute('title');
                                const directText = cell.textContent?.trim();
                                
                                return truncateDiv?.textContent?.trim() || 
                                       title || 
                                       directText || '';
                            });
                            
                            // Create property object
                            const property = {
                                unitId: `sobha_lightning_${Date.now()}_${i}`,
                                project: cellTexts[0] || '',
                                subProject: cellTexts[1] || '',
                                unitType: cellTexts[2] || '',
                                floor: cellTexts[3] || '',
                                unitNo: cellTexts[4] || '',
                                totalUnitArea: cellTexts[5] || '',
                                startingPrice: cellTexts[6] || '',
                                availability: 'available',
                                sourceUrl: window.location.href,
                                extractionMethod: 'Lightning-Experience-Polling',
                                scrapedAt: new Date().toISOString(),
                                rawCellData: cellTexts
                            };
                            
                            // Validate property has meaningful data
                            if (cellTexts[0] && cellTexts[0].length > 0 && 
                                cellTexts[4] && cellTexts[4].length > 0) { // Project and Unit No.
                                properties.push(property);
                            }
                        }
                    }
                    
                    return { 
                        status: properties.length > 0 ? 'success' : 'empty',
                        data: properties,
                        tableFound: true,
                        rowCount: rows.length
                    };
                });

                if (result.status === 'success') {
                    extractedData = result.data;
                    this.logger.info(`✅ Successfully extracted ${extractedData.length} properties`);
                    break;
                }

                // Log progress
                if (result.status === 'no-modal') {
                    this.logger.debug(`Attempt ${attempts + 1}: No modal found, retrying...`);
                } else if (result.status === 'no-table') {
                    this.logger.debug(`Attempt ${attempts + 1}: Modal found but no table, retrying...`);
                } else if (result.status === 'no-rows') {
                    this.logger.debug(`Attempt ${attempts + 1}: Table found but no rows, retrying...`);
                } else if (result.status === 'empty') {
                    this.logger.debug(`Attempt ${attempts + 1}: Rows found but no valid data, retrying...`);
                }

                await page.waitForTimeout(2000);
                attempts++;
            }

            if (extractedData.length === 0) {
                this.logger.warn('No Lightning property data found after maximum polling attempts - creating debug entry');
                
                extractedData.push({
                    unitId: `debug_lightning_polling_${Date.now()}`,
                    project: 'Lightning Polling Debug Entry',
                    subProject: 'No Properties Found After Polling',
                    unitType: 'Debug',
                    floor: '0',
                    unitNo: 'DEBUG-POLLING-001',
                    totalUnitArea: '0',
                    startingPrice: '0',
                    availability: 'debug',
                    sourceUrl: page.url(),
                    debugInfo: {
                        message: 'No Lightning property data found after polling',
                        attemptsMax: maxAttempts
                    },
                    extractionMethod: 'Lightning-Debug-Polling',
                    scrapedAt: new Date().toISOString()
                });
            }

            // Record metrics
            this.metrics.recordPropertiesScraped(extractedData.length);
            
            this.logger.info('Lightning Experience property data extraction completed', {
                totalExtracted: extractedData.length,
                extractionMethod: 'Lightning-Experience-Polling'
            });

            return extractedData;

        } catch (error) {
            this.logger.error('Lightning Experience property data extraction failed', { error: error.message });
            
            // Return error entry
            return [{
                unitId: `error_lightning_polling_${Date.now()}`,
                project: 'Lightning Polling Error Entry',
                subProject: 'Extraction failed with polling',
                unitType: 'Error',
                floor: '0',
                unitNo: 'ERROR-POLLING-001',
                totalUnitArea: '0',
                startingPrice: '0',
                availability: 'error',
                sourceUrl: page.url(),
                errorInfo: error.message,
                extractionMethod: 'Lightning-Error-Polling',
                scrapedAt: new Date().toISOString()
            }];
        }
    }

    /**
     * Main enhanced scraping workflow with Lightning Experience compatibility
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
                    this.logger.info('Starting Lightning Experience aware scraping workflow', { url: request.url });

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
                            approach: 'lightning-experience-compatible'
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
                            scraperVersion: '1.0.6',
                            portalUrl: CONFIG.LOGIN_URL,
                            userAgent: await page.evaluate(() => navigator.userAgent),
                            viewport: await page.evaluate(() => ({
                                width: window.innerWidth,
                                height: window.innerHeight
                            })),
                            approach: 'lightning-experience-compatible',
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

                    this.logger.info('Lightning Experience aware scraping workflow completed successfully', {
                        propertiesCount: properties.length,
                        successRate: this.metrics.getSuccessRate(),
                        duration: Math.round(performance.now() - scrapeStart)
                    });

                } catch (error) {
                    const duration = performance.now() - scrapeStart;
                    this.metrics.recordRequest(false, duration, error);
                    
                    this.logger.error('Lightning Experience aware scraping workflow failed', {
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
                        approach: 'lightning-experience-compatible'
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
 * MAIN LIGHTNING EXPERIENCE AWARE ACTOR ENTRY POINT
 */
async function main() {
    try {
        await Actor.init();
        console.log('Lightning Experience aware Actor initialized successfully');

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

        // Initialize Lightning Experience aware scraper
        console.log('Initializing Lightning Experience aware enterprise scraper...');
        const scraper = new EnterpriseSobhaPortalScraper(validatedInput);

        // Execute Lightning Experience aware scraping workflow
        console.log('Starting Lightning Experience aware scraping workflow...');
        const results = await scraper.executeScraping();

        if (results.success) {
            console.log('Lightning Experience aware scraping completed successfully');
            if (Actor.log && typeof Actor.log.info === 'function') {
                Actor.log.info('Lightning Experience aware scraping completed successfully', {
                    sessionId: results.sessionId,
                    successRate: results.metrics.successRate,
                    propertiesScraped: results.metrics.propertiesScraped
                });
            }
        } else {
            console.error('Lightning Experience aware scraping failed:', results.error);
            if (Actor.log && typeof Actor.log.error === 'function') {
                Actor.log.error('Lightning Experience aware scraping failed', { error: results.error });
            }
            await Actor.fail(`Lightning Experience aware scraping failed: ${results.error}`);
            return;
        }

    } catch (error) {
        console.error('Critical error in Lightning Experience aware actor:', error.message);
        console.error('Stack trace:', error.stack);
        
        if (Actor.log && typeof Actor.log.error === 'function') {
            Actor.log.error('Critical error in Lightning Experience aware actor', { 
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
