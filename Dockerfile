# Use the official Apify image with Node.js 18 and Playwright support
FROM apify/actor-node-playwright:18

# Copy the package.json and package-lock.json files to the working directory
COPY package*.json ./

# Install npm dependencies
# Using --omit=dev ensures that only production dependencies are installed.
RUN npm install --omit=dev

# Copy the rest of the actor's source code
COPY . .

# Run the actor
CMD [ "npm", "start" ]
