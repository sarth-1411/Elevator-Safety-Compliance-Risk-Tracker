%sql
CREATE OR REPLACE VIEW elevator.model.vw_compliance_queue AS
SELECT
  f.Device_Number,
  f.BIN,
  f.Borough,
  f.Compliance_Status,
  CAST(f.Next_Due_Date AS STRING) AS Next_Due_Date,
  f.Days_Until_Due,
  f.Open_Violation_Count
FROM elevator.model.fact_compliance f
WHERE f.Compliance_Status IN ('Overdue','Due Soon')
ORDER BY f.Days_Until_Due ASC
LIMIT 200;