SECURITY:
- remove apiKey: domain.apiKey from all logs
- replace all promises to async/await accross whole codebase and add try catch blocks to all async functions

PERFORMANCE:
- make queue as sigleton, not pass accross param, use as service 
- run processContacts, processCompanies, processMeetings in parallel

STRUCTURE:
- moved pagination logic to a separate service/function
- moved retry logic to a separate service/function
- moved processContacts, processCompanies, processMeetings into process.service.js



