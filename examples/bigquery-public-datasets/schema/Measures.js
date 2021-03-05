cube(`Measures`, {
  sql: `
    SELECT *
    FROM \`bigquery-public-data.covid19_govt_response.oxford_policy_tracker\`
  `,

  refreshKey: {
    sql: `
      SELECT COUNT(*)
      FROM \`bigquery-public-data.covid19_govt_response.oxford_policy_tracker\`
    `,
  },

  measures: {
    confirmed_cases: {
      sql: `confirmed_cases`,
      type: `max`
    },

    minStringencyIndex: {
      sql: `stringency_index`,
      type: `min`,
      format: 'percent',
    },

    maxStringencyIndex: {
      sql: `stringency_index`,
      type: `max`,
      format: 'percent',
    },

    schoolClosing: {
      sql: `CAST(school_closing AS NUMERIC)`,
      type: `max`
    },

    workplaceClosing: {
      sql: `CAST(workplace_closing AS NUMERIC)`,
      type: `max`
    },

    cancelPublicEvents: { 
      sql: `CAST(cancel_public_events AS NUMERIC)`,
      type: `max`
    },

    restrictionsOnGatherings: { 
      sql: `CAST(restrictions_on_gatherings AS NUMERIC)`,
      type: `max`
    },

    closePublicTransit: {
      sql: `CAST(close_public_transit AS NUMERIC)`,
      type: `max`
    },

    stayAtHomeRequirements: {
      sql: `CAST(stay_at_home_requirements AS NUMERIC)`,
      type: `max`
    },

    restrictionsOnInternalMovement: {
      sql: `CAST(restrictions_on_internal_movement AS NUMERIC)`,
      type: `max`
    },

    internationalTravelControls: { 
      sql: `CAST(international_travel_controls AS NUMERIC)`,
      type: `max`
    },

    incomeSupport: { 
      sql: `CAST(income_support AS NUMERIC)`,
      type: `max`
    },

    debtContractRelief: { 
      sql: `CAST(debt_contract_relief AS NUMERIC)`,
      type: `max`
    },

    fiscalMeasures: { 
      sql: `CAST(fiscal_measures AS NUMERIC)`,
      type: `max`
    },

    internationalSupport: { 
      sql: `CAST(international_support AS NUMERIC)`,
      type: `max`
    },

    publicInformationCampaigns: { 
      sql: `CAST(public_information_campaigns AS NUMERIC)`,
      type: `max`
    },

    testingPolicy: { 
      sql: `CAST(testing_policy AS NUMERIC)`,
      type: `max`
    },

    contactTracing: { 
      sql: `CAST(contact_tracing AS NUMERIC)`,
      type: `max`
    },

    emergencyHealthcareInvestment: { 
      sql: `CAST(emergency_healthcare_investment AS NUMERIC)`,
      type: `max`
    },

    vaccineInvestment: { 
      sql: `CAST(vaccineInvestment AS NUMERIC)`,
      type: `max`
    }
  },

  dimensions: {
    key: {
      sql: `CONCAT(country_name, '-', ${Measures}.date)`,
      type: `string`,
      primaryKey: true
    },

    country: {
      sql: `country_name`,
      type: `string`
    },

    date: {
      sql: `TIMESTAMP(${Measures}.date)`,
      type: `time`
    },
  },
});