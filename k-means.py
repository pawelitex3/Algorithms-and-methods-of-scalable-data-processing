import sys
import math

points = []

f = open("punkty.txt", "r")
for line in f:
    line = line.replace("\n", "")
    x, y = line.split(" ")
    points.append((float(x), float(y)))

centres = []
for i in range(3):
    centres.append([points[i]])

change = True
iterations = 0
while change:
    iterations += 1
    for point in points:
        dist0 = math.sqrt((point[0]-centres[0][0][0])**2 + (point[1]-centres[0][0][1])**2)
        dist1 = math.sqrt((point[0]-centres[1][0][0])**2 + (point[1]-centres[1][0][1])**2)
        dist2 = math.sqrt((point[0]-centres[2][0][0])**2 + (point[1]-centres[2][0][1])**2)

        if dist0 < dist1:
            if dist0 < dist2:
                centres[0].append(point)
            else:
                centres[2].append(point)
        elif dist1 < dist2:
            centres[1].append(point)
        else:
            centres[2].append(point)
    number_of_changes = 0
    averages = []
    for i in range(3):
        average = tuple([sum(y) / len(y) for y in zip(*centres[i][1:])])
        if average != centres[i][0]:
            number_of_changes += 1
        averages.append(average)
    if number_of_changes > 0:
        centres = []
        for i in range(3):
            centres.append([averages[i]])
    else:
        change = False
    

print(centres)
print(iterations)
