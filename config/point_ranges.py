

# Configure point assignments for the different attribute ranges
# Lists of tuples with point value as the first element of the tuple and the inclusive minimum as the second element

range_map = {
    'ED_visits': [(0, float("-inf")), (1, 1), (2, 2), (3, 3), (4, 4)],
    'LengthofStay': [(0, float("-inf")), (1, 1), (2, 2), (3, 3), (4, 4), (5, 7), (7, 14)],
    'ComorbidityScore': [(0, float("-inf")), (1, 1), (2, 2), (3, 3), (5, 4)]
}