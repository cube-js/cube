---
title: Cohort Retention
permalink: /cohort-retention
category: Reference
subCategory: Tutorials
menuOrder: 15
---
[comment]: # (DOCUMENT IS PROOFREADED, MAKE CHANGES WITH CONFIDENCE)

This is an advanced topic that assumes good, pre-existing knowledge of SQL and Cube.js. 

Whether you’re selling groceries, financial services, or gym memberships, successful recruitment of new customers is only 
truly successful if they return to buy from you again. 
The metric that reflects this is called **retention**, and the approach we use is **customer retention analysis**.
Retention analysis is typically done using **cohort analysis**.

Cohort analysis is a technique to see how variables change over in different groups with different starting conditions. 
Retention is a simplified one, where the **starting condition is usually the time of signup and the variable is simply activity**.

It’s usually visualized as a cohort grid or retention curves.

<img src="https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/Schema/cohort-retention1.png" width="100%" />

Cohort retention analysis is pretty hard to do in SQL. **We need to have the user-date combination**, which tells us about 
a user’s activity on that date, including dates with no activity. To do this, 
we need to make a tricky join, which gives us a dates list. Once we have it, we can “fill it” with users’ activities. 

The example below shows monthly cohort retention. The same technique can be used for daily or weekly retention.

<div class="block help-block">
The SQL code in this guide is Postgres compliant. The final SQL code may be different depending on your database. 
Also, this technique requires at least 1 user to be active during the month, otherwise this month will not be included in the months' list.
</div>


```javascript
cube(`MonthlyRetention`, {
 sql:
  `SELECT
     users.id as user_id,
     date_trunc('month', users.created_at) as signup_month,
     months_list.activity_month as activity_month,
     data.monthly_pageviews
   FROM users
   LEFT JOIN
     (
       SELECT
         DISTINCT (date_trunc('month', pages.original_timestamp)) as activity_month
         FROM pages
     ) as months_list
   ON months_list.activity_month >= date_trunc('month', users.created_at)
   LEFT JOIN
     (
       SELECT
         p.user_id,
         date_trunc('month', p.original_timestamp) as activity_month,
         COUNT(DISTINCT p.id) as monthly_pageviews
         FROM pages p
         GROUP BY 1,2
     ) as data
   ON data.activity_month = months_list.activity_month
   AND data.user_id = users.id`,
});
```
The SQL above provides the base table for our retention cube. It would show signup months and activity months with pageviews:

|  **user_id** | **signup_month** | **activity_month** | **mothly_pageviews** |
|  ------ | ------ | ------ | ------ |
|  1 | 1/18 | 1/18 | 10 |
|  1 | 1/18 | 2/18 | 5 |
|  1 | 1/18 | 3/18 | 0 |
|  2 | 2/18 | 2/18 | 12 |
|  2 | 2/18 | 3/18 | 0 |
|  3 | 3/18 | 3/18 | 5 |

Now we can calculate a total count of users and the total count of active users, who has more than 0 page views, 
for every month. Based on these two measures we can calculate monthly `percentageOfActive`.

```javascript
cube(`monthlyRetention`, {
 measures: {
   totalCount: {
     sql: `user_id`,
     type: `countDistinct`,
     shown: false
   },
  
   totalActiveCount: {
     sql: `user_id`,
     type: `countDistinct`,
     drillMembers: [Users.id, Users.email],
     filters: [
       { sql: `${CUBE}.monthly_pageviews > 0`}
     ]
   },
  
   percentageOfActive: {
     sql: `100.0 * ${totalActiveCount} / nullif(${totalCount}, 0)`,
     type: `number`,
     format: `percent`,
     drillMembers: [Users.email, bots.team, bots.lastSeen, percentageOfActive],
   }
 }
});
```

To be able to build cohorts, we need to group by two dimensions: **signup date**, which will define our cohorts, and **months since signup**, which will show how the percentage of active users is changing.

```javascript
cube(`monthlyRetention`, {
 dimensions: {
   monthsSinceSignup: {
     sql: `DATEDIFF('month', ${CUBE}.signup_month, ${CUBE}.activity_month)`,
     type: `number`
   },
  
   signupDate: {
     sql: `(signup_month AT TIME ZONE 'America/Los_Angeles')`,
     type: `time`
   }
 }
});
```
<div class="block help-block">
Note, we are explicitly setting the `signupMonth` timezone. `date_trunc` returns UTC dates and not setting a correct 
timezone would lead to wrong results due to time shift.
</div>
