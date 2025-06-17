SECURITY:
- remove apiKey: domain.apiKey from all logs
- replace all promises to async/await accross whole codebase and add try catch blocks to all async functions

PERFORMANCE:
- make queue as sigleton, not pass accross param, use as service 
- run processContacts, processCompanies, processMeetings in parallel not in sequence
- add cache for entities like associations, mappings like contactId-to-email, contact and etc to avoid unnecessary API calls 
- skip saving of entities if exist in db (or cache)

STRUCTURE:
- moved pagination logic to a separate service/function
- moved retry logic to a separate service/function
- moved processContacts, processCompanies, processMeetings into process.service.js
- moved refreshAccessToken into hubspot.service.js
- moved mapping logic to a separate functions and rewrite them in more readable way

BUGS:
- QUEUE_CONCURRENCY = 100000000 is too high, possibly causing memory issues, between start with something reasonable 10-50
- 

