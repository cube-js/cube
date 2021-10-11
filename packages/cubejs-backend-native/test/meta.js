module.exports = {
    "cubes": [
        {
            "name": "KibanaSampleDataEcommerce",
            "title": "Ecommerce",
            "measures": [
                {
                    "name": "KibanaSampleDataEcommerce.count",
                    "title": "Ecommerce Count",
                    "shortTitle": "Count",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "count",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.maxPrice",
                    "title": "Ecommerce Max Price",
                    "shortTitle": "Max Price",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "max",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.minPrice",
                    "title": "Ecommerce Min Price",
                    "shortTitle": "Min Price",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "min",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.avgPrice",
                    "title": "Ecommerce Avg Price",
                    "shortTitle": "Avg Price",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "avg",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                }
            ],
            "dimensions": [
                {
                    "name": "KibanaSampleDataEcommerce.order_date",
                    "title": "Ecommerce Order Date",
                    "type": "time",
                    "shortTitle": "Order Date",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.customer_gender",
                    "title": "Ecommerce Customer Gender",
                    "type": "string",
                    "shortTitle": "Customer Gender",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.taxful_total_price",
                    "title": "Ecommerce Taxful Total Price",
                    "type": "number",
                    "shortTitle": "Taxful Total Price",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "KibanaSampleDataEcommerce.taxless_total_price",
                    "title": "Ecommerce Taxless Total Price",
                    "type": "number",
                    "shortTitle": "Taxless Total Price",
                    "suggestFilterValues": true,
                    "isVisible": true
                }
            ],
            "segments": [
                {
                    "name": "KibanaSampleDataEcommerce.is_male",
                    "title": "Ecommerce Is Male",
                    "shortTitle": "Is Male"
                },
                {
                    "name": "KibanaSampleDataEcommerce.is_female",
                    "title": "Ecommerce Is Female",
                    "shortTitle": "Is Female"
                }
            ]
        },
        {
            "name": "Logs",
            "title": "Logs",
            "measures": [
                {
                    "name": "Logs.count",
                    "title": "Logs Count",
                    "shortTitle": "Count",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "count",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                },
                {
                    "name": "Logs.agentCount",
                    "title": "Logs Agent Count",
                    "shortTitle": "Agent Count",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "countDistinct",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                },
                {
                    "name": "Logs.agentCountApprox",
                    "title": "Logs Agent Count Approx",
                    "shortTitle": "Agent Count Approx",
                    "cumulativeTotal": false,
                    "cumulative": false,
                    "type": "number",
                    "aggType": "countDistinctApprox",
                    "drillMembers": [],
                    "drillMembersGrouped": {
                        "measures": [],
                        "dimensions": []
                    },
                    "isVisible": true
                }
            ],
            "dimensions": [
                {
                    "name": "Logs.agent",
                    "title": "Logs Agent",
                    "type": "string",
                    "shortTitle": "Agent",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "Logs.referer",
                    "title": "Logs Referer",
                    "type": "string",
                    "shortTitle": "Referer",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "Logs.host",
                    "title": "Logs Host",
                    "type": "string",
                    "shortTitle": "Host",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "Logs.tags",
                    "title": "Logs Tags",
                    "type": "string",
                    "shortTitle": "Tags",
                    "suggestFilterValues": true,
                    "isVisible": true
                },
                {
                    "name": "Logs.timestamp",
                    "title": "Logs Timestamp",
                    "type": "time",
                    "shortTitle": "Timestamp",
                    "suggestFilterValues": true,
                    "isVisible": true
                }
            ],
            "segments": []
        }
    ]
};
