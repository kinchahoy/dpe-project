Key principles
    -   Predictable behavior
    -   Proactive, not reactive
    -   Adapt to developing circumstances

Productionization challenges
    -   Scale to real DB (e.g. postgres)
    -   Product level PII (hashed creditcard ID, user ids)
    -   Richer machine level maintence and tracking
    -   Pipeline to injest transactions, machine status
    -   Pipeline to generate price changes, stock quantities etc.

Productionization notes

Database will need
- Richer location support including timezones, regions
- Machines will need a maintence history

Optimizations
- Composite indexes for fast search etc.

- I'm using simple python transformations of the dataset now because they are easy to extend to production grade (triggers, etc.)
