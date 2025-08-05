/**
 * Enterprise Router Configuration for Sobha Portal Scraper
 * ======================================================
 * Production-ready routing handlers with comprehensive error management
 * 
 * Author: BARACA Engineering Team
 * Version: 1.0.0
 * License: Proprietary - BARACA Life Capital Real Estate
 */

import { createPlaywrightRouter } from 'crawlee';

export const router = createPlaywrightRouter();

/**
 * Default handler for Sobha Portal processing
 * This handles the main scraping workflow
 */
router.addDefaultHandler(async ({ page, request, log }) => {
    const sessionId = Math.random().toString(36).substring(2, 15);
    
    log.info('Processing Sobha Portal request', {
        url: request.url,
        sessionId,
        timestamp: new Date().toISOString()
    });

    try {
        // Basic page validation
        if (!page) {
            throw new Error('Page object is not available');
        }

        // Log page title for debugging
        const pageTitle = await page.title();
        log.info('Page loaded successfully', { 
            title: pageTitle,
            sessionId 
        });

        // The actual processing is handled by the main scraper logic in main.js
        // This router just provides the entry point and basic error handling
        
        log.info('Router processed successfully', { 
            sessionId,
            pageTitle 
        });
        
    } catch (error) {
        log.error('Router processing failed', {
            error: error.message,
            sessionId,
            url: request.url,
            stack: error.stack
        });
        throw error;
    }
});

/**
 * Error handler for failed requests
 * Handles all request failures with comprehensive logging
 */
router.addHandler('ERROR', async ({ request, log }) => {
    log.error('Request failed in router', {
        url: request.url,
        retryCount: request.retryCount,
        timestamp: new Date().toISOString(),
        loadedUrl: request.loadedUrl,
        errorMessages: request.errorMessages || []
    });

    // Log additional debugging information
    if (request.retryCount > 0) {
        log.warn('Request retry detected', {
            url: request.url,
            retryCount: request.retryCount,
            maxRetries: 3
        });
    }
});

/**
 * Success handler for completed requests
 * Logs successful request completions
 */
router.addHandler('SUCCESS', async ({ request, log }) => {
    log.info('Request completed successfully in router', {
        url: request.url,
        timestamp: new Date().toISOString(),
        loadedUrl: request.loadedUrl
    });
});

/**
 * Handler for Sobha login page
 * Specifically handles login page routing
 */
router.addHandler('LOGIN', async ({ page, request, log }) => {
    log.info('Handling Sobha login page', {
        url: request.url
    });

    try {
        // Wait for login form to be available
        await page.waitForSelector('input[type="email"]', { timeout: 10000 });
        
        log.info('Login page loaded successfully', {
            url: request.url
        });

    } catch (error) {
        log.error('Login page handling failed', {
            error: error.message,
            url: request.url
        });
        throw error;
    }
});

/**
 * Handler for Sobha projects page
 * Handles the projects listing page routing
 */
router.addHandler('PROJECTS', async ({ page, request, log }) => {
    log.info('Handling Sobha projects page', {
        url: request.url
    });

    try {
        // Wait for projects page elements
        await page.waitForSelector('text=Select Bed', { timeout: 15000 });
        
        log.info('Projects page loaded successfully', {
            url: request.url
        });

    } catch (error) {
        log.error('Projects page handling failed', {
            error: error.message,
            url: request.url
        });
        throw error;
    }
});

/**
 * Generic error handler for unhandled routes
 */
router.addHandler('UNHANDLED', async ({ request, log }) => {
    log.warn('Unhandled route encountered', {
        url: request.url,
        label: request.userData?.label || 'unknown'
    });
});
