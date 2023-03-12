Feature: fault injector
    Check test framework

Scenario: Activate fault point
    Given I start postgres0
    Then postgres0 is a leader after 10 seconds
    Given I start postgres1
	Then "members/postgres1" key in DCS has state=running after 10 seconds
    Given I inject fault 'test' into postgres0
    Then fault point 'test' is activated in postgres0
    Given I inject fault 'test' (type=exception) into postgres1
    Then fault point 'test' is activated in postgres1
    Given I inject fault 'test_exception' (type=exception, start_from=3, end_after=4) into postgres0
    Then fault point 'test_exception' is activated in postgres0

Scenario: Activate fault point of type sleep
    Given I inject fault 'test_sleep' (type=sleep, sleep_time=42) into postgres0
    Then fault point 'test_sleep' is activated in postgres0
    Given I inject fault 'test_sleep' (type=sleep, start_from=3, end_after=4, sleep_time=42) into postgres1
    Then fault point 'test_sleep' is activated in postgres1

Scenario: Deactivate fault point(s)
    Given I deactivate fault point 'test' in postgres0
    Then fault point 'test' is not activated in postgres0
    And fault point 'test' is activated in postgres1
    Given I deactivate all fault points in postgres0
    Then there are no activated fault points in postgres0
    Given I deactivate all fault points
    Then there are no activated fault points in postgres1
