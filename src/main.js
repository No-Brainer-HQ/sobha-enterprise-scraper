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
                        
                        // First, look for the specific Lightning component modal
                        const lightningComponentModal = document.querySelector('[c-brokerportalhomepage_brokerportalhomepage]');
                        if (lightningComponentModal) {
                            console.log('Found Lightning component modal');
                            
                            // Look for close buttons within the Lightning component
                            const closeButtons = lightningComponentModal.querySelectorAll('button');
                            console.log(`Lightning component has ${closeButtons.length} buttons`);
                            
                            for (const button of closeButtons) {
                                const text = button.textContent || button.innerHTML || '';
                                const ariaLabel = button.getAttribute('aria-label') || '';
                                const title = button.getAttribute('title') || '';
                                const className = button.className || '';
                                
                                // Check if it's a close button
                                if (text.includes('×') || 
                                    ariaLabel.toLowerCase().includes('close') || 
                                    title.toLowerCase().includes('close') ||
                                    className.includes('slds-modal__close') ||
                                    className.includes('slds-button_icon')) {
                                    
                                    console.log(`Found Lightning component close button: ${text || ariaLabel || title || 'icon button'}`);
                                    button.click();
                                    return true;
                                }
                            }
                        }
                        
                        // Fallback: Look for modal containers generally
                        const modals = document.querySelectorAll('[role="dialog"], .slds-modal');
                        console.log(`Found ${modals.length} modal containers`);
                        
                        for (const modal of modals) {
                            // Look for close buttons within each modal
                            const closeButtons = modal.querySelectorAll('button');
                            console.log(`Modal has ${closeButtons.length} buttons`);
                            
                            for (const button of closeButtons) {
                                const text = button.textContent || button.innerHTML || '';
                                const ariaLabel = button.getAttribute('aria-label') || '';
                                const title = button.getAttribute('title') || '';
                                
                                // Check if it's a close button
                                if (text.includes('×') || 
                                    ariaLabel.toLowerCase().includes('close') || 
                                    title.toLowerCase().includes('close') ||
                                    button.classList.contains('slds-modal__close')) {
                                    
                                    console.log(`Found close button: ${text || ariaLabel || title}`);
                                    button.click();
                                    return true;
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
     * ENHANCED: Open property modal with Lightning component awareness
     */
    async openPropertyModal(page) {
        try {
            this.logger.info('Opening property listings modal with Lightning awareness');

            // Wait for Lightning components to be fully interactive
            await page.waitForTimeout(3000);

            // DEBUGGING: Analyze what's actually on the page AFTER modal dismissal and Lightning rendering
            this.logger.info('Analyzing page content after modal dismissal and Lightning rendering');
            const pageAnalysis = await page.evaluate(() => {
                const buttons = Array.from(document.querySelectorAll('button'));
                const inputs = Array.from(document.querySelectorAll('input'));
                const allClickables = Array.from(document.querySelectorAll('button, input[type="button"], input[type="submit"], [onclick], [role="button"]'));
                
                // Look specifically for Lightning component content
                const sobhaComponent = document.querySelector('c-brokerportalsohbaprojects, [class*="brokerportalsohbaprojects"]');
                const lightningElements = document.querySelectorAll('[class*="slds-"], [data-aura-rendered-by]');
                
                return {
                    totalButtons: buttons.length,
                    totalInputs: inputs.length,
                    totalClickables: allClickables.length,
                    hasSobhaComponent: !!sobhaComponent,
                    lightningElementCount: lightningElements.length,
                    buttonTexts: buttons.map(btn => btn.textContent?.trim()).filter(text => text && text.length > 0),
                    inputValues: inputs.map(inp => inp.value?.trim() || inp.placeholder?.trim()).filter(text => text && text.length > 0),
                    clickableTexts: allClickables.map(el => el.textContent?.trim() || el.value?.trim() || el.getAttribute('aria-label')).filter(text => text && text.length > 0),
                    pageText: document.body.textContent?.includes('Filter') ? 'Contains Filter text' : 'No Filter text found',
                    pageTitle: document.title,
                    currentUrl: window.location.href,
                    
                    // Check for remaining modals (including Lightning component modal)
                    visibleModals: Array.from(document.querySelectorAll('[role="dialog"], .slds-modal, [c-brokerportalhomepage_brokerportalhomepage]'))
                        .filter(modal => modal.offsetParent !== null).length,
                    modalInfo: Array.from(document.querySelectorAll('[role="dialog"], .slds-modal, [c-brokerportalhomepage_brokerportalhomepage]'))
                        .filter(modal => modal.offsetParent !== null)
                        .map(modal => ({
                            className: modal.className,
                            componentName: modal.getAttribute('c-brokerportalhomepage_brokerportalhomepage') ? 'Lightning-HomePage' : 'Standard',
                            textContent: (modal.textContent || '').substring(0, 100)
                        })),
                    
                    // Sample of actual button elements for debugging
                    buttonSample: buttons.slice(0, 5).map(btn => ({
                        text: btn.textContent?.trim(),
                        value: btn.value,
                        className: btn.className,
                        id: btn.id,
                        type: btn.type,
                        visible: btn.offsetParent !== null
                    }))
                };
            });

            this.logger.info('Lightning-aware page analysis completed', pageAnalysis);

            // Find and click the "Filter Properties" button
            this.logger.info('Looking for Filter Properties button with Lightning awareness');
            
            try {
                // Enhanced selector targeting Lightning-rendered content
                const lightningFilterSelectors = [
                    // Standard button text
                    'button:has-text("Filter Properties")',
                    
                    // Lightning-specific button selectors
                    'lightning-button:has-text("Filter Properties")',
                    'lightning-button:has-text("Filter")',
                    
                    // Aura component buttons
                    '[data-aura-class*="button"]:has-text("Filter")',
                    
                    // SLDS (Salesforce Lightning Design System) buttons
                    '.slds-button:has-text("Filter")',
                    '.slds-button:has-text("Properties")',
                    
                    // Generic Lightning buttons with filter text
                    'button:has-text("Filter")',
                    'button:has-text("Apply")',
                    'button:has-text("Search")',
                    
                    // Try different case variations
                    'button:has-text("FILTER PROPERTIES")',
                    'button:has-text("filter properties")',
                    'button:has-text("Apply Filter")',
                    'button:has-text("Search Properties")'
                ];

                let buttonFound = false;
                for (const selector of lightningFilterSelectors) {
                    try {
                        this.logger.debug(`Trying Lightning selector: ${selector}`);
                        
                        // Wait for the element with a reasonable timeout
                        await page.waitForSelector(selector, { 
                            timeout: 5000,
                            state: 'visible'
                        });

                        this.logger.info(`Found button with Lightning selector: ${selector}`);
                        
                        // Click the button
                        await page.click(selector);
                        buttonFound = true;
                        break;
                        
                    } catch (selectorError) {
                        this.logger.debug(`Lightning selector failed: ${selector}`, { error: selectorError.message });
                    }
                }

                if (buttonFound) {
                    // Wait for modal to appear
                    this.logger.info('Button clicked, waiting for property modal to load');
                    await page.waitForSelector(CONFIG.SELECTORS.propertyModal, { 
                        timeout: CONFIG.MODAL_WAIT,
                        state: 'visible'
                    });

                    // Additional wait for modal content to load
                    await page.waitForTimeout(3000);

                    this.logger.info('✅ Property modal opened successfully');
                    return true;
                } else {
                    throw new Error('No Lightning filter button selectors worked');
                }

            } catch (buttonError) {
                this.logger.error('Lightning-aware button detection failed', { 
                    error: buttonError.message 
                });
                
                // FINAL ATTEMPT: Use JavaScript to find and click any filter-related button
                this.logger.info('Trying comprehensive JavaScript button detection');
                try {
                    const filterButtonClicked = await page.evaluate(() => {
                        console.log('Starting comprehensive button search...');
                        
                        // Get all potentially clickable elements
                        const allElements = Array.from(document.querySelectorAll(
                            'button, input[type="button"], input[type="submit"], [role="button"], [onclick], lightning-button'
                        ));
                        
                        console.log(`Found ${allElements.length} potentially clickable elements`);
                        
                        for (const element of allElements) {
                            const text = (element.textContent || element.value || '').toLowerCase();
                            const ariaLabel = (element.getAttribute('aria-label') || '').toLowerCase();
                            const className = (element.className || '').toLowerCase();
                            const id = (element.id || '').toLowerCase();
                            
                            // Look for filter-related keywords
                            const searchTerms = ['filter', 'search', 'apply', 'properties', 'submit'];
                            const hasFilterKeyword = searchTerms.some(term => 
                                text.includes(term) || ariaLabel.includes(term) || className.includes(term) || id.includes(term)
                            );
                            
                            if (hasFilterKeyword && element.offsetParent !== null) { // Visible element
                                console.log(`Found potential filter button: "${text || ariaLabel || className}" - attempting click`);
                                
                                try {
                                    element.click();
                                    console.log('Button clicked successfully');
                                    return true;
                                } catch (clickError) {
                                    console.log(`Click failed: ${clickError}`);
                                }
                            }
                        }
                        
                        console.log('No suitable filter button found');
                        return false;
                    });
                    
                    if (filterButtonClicked) {
                        this.logger.info('Filter button clicked via JavaScript');
                        await page.waitForTimeout(5000);
                        
                        // Check if modal appeared
                        const modalCount = await page.locator(CONFIG.SELECTORS.propertyModal).count();
                        if (modalCount > 0) {
                            this.logger.info('✅ Modal opened with JavaScript button detection');
                            return true;
                        } else {
                            this.logger.warn('Button was clicked but no modal appeared');
                        }
                    }
                } catch (jsError) {
                    this.logger.debug('JavaScript button detection failed', { error: jsError.message });
                }
                
                throw new Error('Could not find or click Filter Properties button after comprehensive Lightning-aware search');
            }

        } catch (error) {
            this.logger.error('Failed to open property modal with Lightning awareness', { error: error.message });
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
