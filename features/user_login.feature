Feature: Member Data Validation 
  In order to verify correct member data retrieval
  As a QA automation framework
  I want to read the test data from feature files and generate DB queries dynamically

  @ADO-1234
  Scenario Outline: Verify active and registered member data for different member types
    Given the member type "<member_type>"
    And the required condition "<member_criteria>"
    When Ask-DB runs data validation against Oracle and DWH
    Then correct member details should be fetched successfully

    Examples:
      | member_type | member_criteria          |
      | accum       | basic_insurance          |

