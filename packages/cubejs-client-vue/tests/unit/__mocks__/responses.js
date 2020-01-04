export const meta = {
  "cubes": [
    {
      "name": "LineItems",
      "title": "Line Items",
      "connectedComponent": 1,
      "measures": [
        {
          "name": "LineItems.count",
          "title": "Line Items Count",
          "shortTitle": "Count",
          "aliasName": "line_items.count",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number",
          "drillMembers": [
            "LineItems.id",
            "LineItems.createdAt"
          ]
        },
        {
          "name": "LineItems.quantity",
          "title": "Line Items Quantity",
          "shortTitle": "Quantity",
          "aliasName": "line_items.quantity",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number"
        },
        {
          "name": "LineItems.price",
          "title": "Line Items Price",
          "shortTitle": "Price",
          "aliasName": "line_items.price",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number"
        }
      ],
      "dimensions": [
        {
          "name": "LineItems.createdAt",
          "title": "Line Items Created at",
          "type": "time",
          "aliasName": "line_items.created_at",
          "shortTitle": "Created at",
          "suggestFilterValues": true
        }
      ],
      "segments": []
    },
    {
      "name": "Orders",
      "title": "Orders",
      "connectedComponent": 1,
      "measures": [
        {
          "name": "Orders.count",
          "title": "Orders Count",
          "shortTitle": "Count",
          "aliasName": "orders.count",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number",
          "drillMembers": [
            "Orders.id",
            "Orders.createdAt"
          ]
        },
        {
          "name": "Orders.number",
          "title": "Orders Number",
          "shortTitle": "Number",
          "aliasName": "orders.number",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number"
        }
      ],
      "dimensions": [
        {
          "name": "Orders.status",
          "title": "Orders Status",
          "type": "string",
          "aliasName": "orders.status",
          "shortTitle": "Status",
          "suggestFilterValues": true
        },
        {
          "name": "Orders.createdAt",
          "title": "Orders Created at",
          "type": "time",
          "aliasName": "orders.created_at",
          "shortTitle": "Created at",
          "suggestFilterValues": true
        },
        {
          "name": "Orders.completedAt",
          "title": "Orders Completed at",
          "type": "time",
          "aliasName": "orders.completed_at",
          "shortTitle": "Completed at",
          "suggestFilterValues": true
        }
      ],
      "segments": []
    },
    {
      "name": "Users",
      "title": "Users",
      "connectedComponent": 1,
      "measures": [
        {
          "name": "Users.count",
          "title": "Users Count",
          "shortTitle": "Count",
          "aliasName": "users.count",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number",
          "drillMembers": [
            "Users.city",
            "Users.id",
            "Users.createdAt"
          ]
        }
      ],
      "dimensions": [
        {
          "name": "Users.city",
          "title": "Users City",
          "type": "string",
          "aliasName": "users.city",
          "shortTitle": "City",
          "suggestFilterValues": true
        },
        {
          "name": "Users.gender",
          "title": "Users Gender",
          "type": "string",
          "aliasName": "users.gender",
          "shortTitle": "Gender",
          "suggestFilterValues": true
        },
        {
          "name": "Users.company",
          "title": "Users Company",
          "type": "string",
          "aliasName": "users.company",
          "shortTitle": "Company",
          "suggestFilterValues": true
        },
        {
          "name": "Users.createdAt",
          "title": "Users Created at",
          "type": "time",
          "aliasName": "users.created_at",
          "shortTitle": "Created at",
          "suggestFilterValues": true
        }
      ],
      "segments": [],
    }
  ]
};

export const load = {
  "query": {
    "measures": [
      "Users.count"
    ],
    "dimensions": [
      "Users.city"
    ],
    "timezone": "UTC",
    "timeDimensions": []
  },
  "data": [
    {
      "Users.city": "Mülheim",
      "Users.count": "4"
    },
    {
      "Users.city": "Metairie",
      "Users.count": "4"
    },
    {
      "Users.city": "Lions Bay",
      "Users.count": "4"
    },
    {
      "Users.city": "Torno",
      "Users.count": "3"
    },
    {
      "Users.city": "Houston",
      "Users.count": "3"
    },
    {
      "Users.city": "Naro",
      "Users.count": "3"
    },
    {
      "Users.city": "Tilly",
      "Users.count": "3"
    },
    {
      "Users.city": "Sooke",
      "Users.count": "3"
    },
    {
      "Users.city": "Kansas City",
      "Users.count": "3"
    },
    {
      "Users.city": "Warren",
      "Users.count": "3"
    },
    {
      "Users.city": "Nice",
      "Users.count": "3"
    },
    {
      "Users.city": "Muzaffarnagar",
      "Users.count": "3"
    },
    {
      "Users.city": "Yeovil",
      "Users.count": "3"
    },
    {
      "Users.city": "Delta",
      "Users.count": "3"
    },
    {
      "Users.city": "Sens",
      "Users.count": "2"
    },
    {
      "Users.city": "Prince George",
      "Users.count": "2"
    },
    {
      "Users.city": "Nederokkerzeel",
      "Users.count": "2"
    },
    {
      "Users.city": "Stigliano",
      "Users.count": "2"
    },
    {
      "Users.city": "Bhind",
      "Users.count": "2"
    },
    {
      "Users.city": "LamontzŽe",
      "Users.count": "2"
    },
    {
      "Users.city": "Portland",
      "Users.count": "2"
    },
    {
      "Users.city": "Morvi",
      "Users.count": "2"
    },
    {
      "Users.city": "Nova Iguaçu",
      "Users.count": "2"
    },
    {
      "Users.city": "Wanganui",
      "Users.count": "2"
    },
    {
      "Users.city": "Ockelbo",
      "Users.count": "2"
    },
    {
      "Users.city": "Gatineau",
      "Users.count": "2"
    },
    {
      "Users.city": "Austin",
      "Users.count": "2"
    },
    {
      "Users.city": "Osogbo",
      "Users.count": "2"
    },
    {
      "Users.city": "Lampeter",
      "Users.count": "2"
    },
    {
      "Users.city": "Camaçari",
      "Users.count": "2"
    },
    {
      "Users.city": "Ruda",
      "Users.count": "2"
    },
    {
      "Users.city": "Cincinnati",
      "Users.count": "2"
    },
    {
      "Users.city": "Ponte San Nicolò",
      "Users.count": "2"
    },
    {
      "Users.city": "Anand",
      "Users.count": "1"
    },
    {
      "Users.city": "Oyace",
      "Users.count": "1"
    },
    {
      "Users.city": "Lillianes",
      "Users.count": "1"
    },
    {
      "Users.city": "Slijpe",
      "Users.count": "1"
    },
    {
      "Users.city": "Castor",
      "Users.count": "1"
    },
    {
      "Users.city": "Curacaví",
      "Users.count": "1"
    },
    {
      "Users.city": "Rawalpindi",
      "Users.count": "1"
    },
    {
      "Users.city": "Saint-Jean-Geest",
      "Users.count": "1"
    },
    {
      "Users.city": "Buckingham",
      "Users.count": "1"
    },
    {
      "Users.city": "Berlare",
      "Users.count": "1"
    },
    {
      "Users.city": "Glovertown",
      "Users.count": "1"
    },
    {
      "Users.city": "Duncan",
      "Users.count": "1"
    },
    {
      "Users.city": "Longano",
      "Users.count": "1"
    },
    {
      "Users.city": "Chatillon",
      "Users.count": "1"
    },
    {
      "Users.city": "Biloxi",
      "Users.count": "1"
    },
    {
      "Users.city": "Saint-Dizier",
      "Users.count": "1"
    },
    {
      "Users.city": "Stintino",
      "Users.count": "1"
    },
    {
      "Users.city": "Raichur",
      "Users.count": "1"
    },
    {
      "Users.city": "Morrinsville",
      "Users.count": "1"
    },
    {
      "Users.city": "St. Albans",
      "Users.count": "1"
    },
    {
      "Users.city": "Argyle",
      "Users.count": "1"
    },
    {
      "Users.city": "Basingstoke",
      "Users.count": "1"
    },
    {
      "Users.city": "Machalí",
      "Users.count": "1"
    },
    {
      "Users.city": "Bernau",
      "Users.count": "1"
    },
    {
      "Users.city": "Logroño",
      "Users.count": "1"
    },
    {
      "Users.city": "D\ufffdgelis",
      "Users.count": "1"
    },
    {
      "Users.city": "Santa María",
      "Users.count": "1"
    },
    {
      "Users.city": "Halkirk",
      "Users.count": "1"
    },
    {
      "Users.city": "Pucón",
      "Users.count": "1"
    },
    {
      "Users.city": "La Baie",
      "Users.count": "1"
    },
    {
      "Users.city": "Merbes-Sainte-Marie",
      "Users.count": "1"
    },
    {
      "Users.city": "Kharagpur",
      "Users.count": "1"
    },
    {
      "Users.city": "Brussel",
      "Users.count": "1"
    },
    {
      "Users.city": "Whitby",
      "Users.count": "1"
    },
    {
      "Users.city": "Devizes",
      "Users.count": "1"
    },
    {
      "Users.city": "Cañas",
      "Users.count": "1"
    },
    {
      "Users.city": "Püttlingen",
      "Users.count": "1"
    },
    {
      "Users.city": "Springfield",
      "Users.count": "1"
    },
    {
      "Users.city": "Pali",
      "Users.count": "1"
    },
    {
      "Users.city": "Glendale",
      "Users.count": "1"
    },
    {
      "Users.city": "Pointe-Claire",
      "Users.count": "1"
    },
    {
      "Users.city": "San Martino in Pensilis",
      "Users.count": "1"
    },
    {
      "Users.city": "Zutphen",
      "Users.count": "1"
    },
    {
      "Users.city": "Victoria",
      "Users.count": "1"
    },
    {
      "Users.city": "San Giorgio Albanese",
      "Users.count": "1"
    },
    {
      "Users.city": "Marburg",
      "Users.count": "1"
    },
    {
      "Users.city": "Salice Salentino",
      "Users.count": "1"
    },
    {
      "Users.city": "Zeveneken",
      "Users.count": "1"
    },
    {
      "Users.city": "West Jordan",
      "Users.count": "1"
    },
    {
      "Users.city": "Sant'Agapito",
      "Users.count": "1"
    },
    {
      "Users.city": "Sadiqabad",
      "Users.count": "1"
    },
    {
      "Users.city": "Wetzlar",
      "Users.count": "1"
    },
    {
      "Users.city": "Laces/Latsch",
      "Users.count": "1"
    },
    {
      "Users.city": "Ashoknagar-Kalyangarh",
      "Users.count": "1"
    },
    {
      "Users.city": "Pelago",
      "Users.count": "1"
    },
    {
      "Users.city": "Oevel",
      "Users.count": "1"
    },
    {
      "Users.city": "Olivar",
      "Users.count": "1"
    },
    {
      "Users.city": "Diets-Heur",
      "Users.count": "1"
    },
    {
      "Users.city": "Cockburn",
      "Users.count": "1"
    },
    {
      "Users.city": "Saint-Prime",
      "Users.count": "1"
    },
    {
      "Users.city": "Orta San Giulio",
      "Users.count": "1"
    },
    {
      "Users.city": "Tiarno di Sopra",
      "Users.count": "1"
    },
    {
      "Users.city": "Zuienkerke",
      "Users.count": "1"
    },
    {
      "Users.city": "Kungälv",
      "Users.count": "1"
    },
    {
      "Users.city": "Mérignac",
      "Users.count": "1"
    },
    {
      "Users.city": "Durness",
      "Users.count": "1"
    },
    {
      "Users.city": "Newtonmore",
      "Users.count": "1"
    },
    {
      "Users.city": "Lier",
      "Users.count": "1"
    },
    {
      "Users.city": "Cles",
      "Users.count": "1"
    },
    {
      "Users.city": "Chiaromonte",
      "Users.count": "1"
    },
    {
      "Users.city": "Genappe",
      "Users.count": "1"
    },
    {
      "Users.city": "Perugia",
      "Users.count": "1"
    },
    {
      "Users.city": "Modakeke",
      "Users.count": "1"
    },
    {
      "Users.city": "Poulseur",
      "Users.count": "1"
    },
    {
      "Users.city": "Gagliano del Capo",
      "Users.count": "1"
    },
    {
      "Users.city": "Paradise",
      "Users.count": "1"
    },
    {
      "Users.city": "San Fratello",
      "Users.count": "1"
    },
    {
      "Users.city": "Bersillies-l'Abbaye",
      "Users.count": "1"
    },
    {
      "Users.city": "Dieppe",
      "Users.count": "1"
    },
    {
      "Users.city": "Massimino",
      "Users.count": "1"
    },
    {
      "Users.city": "Sachs Harbour",
      "Users.count": "1"
    },
    {
      "Users.city": "Altidona",
      "Users.count": "1"
    },
    {
      "Users.city": "Richmond",
      "Users.count": "1"
    },
    {
      "Users.city": "Boorsem",
      "Users.count": "1"
    },
    {
      "Users.city": "Schoonaarde",
      "Users.count": "1"
    },
    {
      "Users.city": "Garaguso",
      "Users.count": "1"
    },
    {
      "Users.city": "Rovereto",
      "Users.count": "1"
    },
    {
      "Users.city": "Saint-Pierre",
      "Users.count": "1"
    },
    {
      "Users.city": "Jerez de la Frontera",
      "Users.count": "1"
    },
    {
      "Users.city": "Cropalati",
      "Users.count": "1"
    },
    {
      "Users.city": "Siculiana",
      "Users.count": "1"
    },
    {
      "Users.city": "Trani",
      "Users.count": "1"
    },
  ],
  "annotation": {
    "measures": {
      "Users.count": {
        "title": "Users Count",
        "shortTitle": "Count",
        "type": "number"
      }
    },
    "dimensions": {
      "Users.city": {
        "title": "Users City",
        "shortTitle": "City",
        "type": "string"
      }
    },
    "segments": {},
    "timeDimensions": {}
  }
};

export const single = {
  "query": {
    "measures": [
      "Users.count"
    ],
    "dimensions": [],
    "timezone": "UTC",
    "timeDimensions": []
  },
  "data": [
    {
      "Users.city": "Mülheim",
      "Users.count": "4"
    },
    {
      "Users.city": "Metairie",
      "Users.count": "4"
    },
  ],
  "annotation": {
    "measures": {
      "Users.count": {
        "title": "Users Count",
        "shortTitle": "Count",
        "type": "number"
      }
    },
    "dimensions": {},
    "segments": {},
    "timeDimensions": {}
  }
};

export const sql = {};

export default (body = {}, status = 200) => () => Promise.resolve({
  status,
  json: () => Promise.resolve(body),
});
