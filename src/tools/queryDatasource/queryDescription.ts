export const queryDatasourceToolDescription = `Use this tool to answer questions about data in the Tableau datasource.
  Always call this tool whenever the user asks a question that requires live data,
  metrics, numbers, or aggregations from Tableau.
  
  Args:
    - datasourceLuid: the Tableau datasource LUID.
    - naturalLanguageQuestion: a plain English question about the data.

  The tool will translate the question into a Tableau query and return the result.`;