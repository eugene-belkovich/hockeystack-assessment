- make queue as sigleton, not pass accross param, use as service 
- run processContacts, processCompanies, processMeetings in parallel

- moved pagination logic to a separate service/function
- moved retry logic to a separate service/function
- moved processContacts, processCompanies, processMeetings into process.service.js

- replace all promises to async/await accross whole codebase
- add try catch blocks to all async functions

