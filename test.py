import numpy as np
import pandas as pd
import requests
from Levenshtein import distance, jaro, jaro_winkler
from scipy.spatial.distance import cdist

df = pd.DataFrame(
  {"submitter": ["Pavel Nadolsky", "Louis Theran", "Hongjun Pan", "Alberto Torchinsky"], "authorsParsed": [
    [["Bal\u00e1zs", "C.", ""], ["Berger", "E. L.", ""], ["Nadolsky", "P. M.", ""], ["Yuan", "C. -P.", ""]],
    [["Streinu", "Ileana", ""], ["Theran", "Louis", ""]], [["Pan", "Hongjun", ""]],
    [["Abu-Shammala", "Wael", ""], ["Torchinsky", "Alberto", ""]]]})

print(df.head())

all_authors = df["authorsParsed"].explode().apply(lambda names: ' '.join(names[1:]) + names[0]).to_numpy()
all_submitters = df["submitter"].to_numpy()

print(all_authors)
print(all_submitters)
print()

distances = cdist(all_authors.reshape(-1, 1), all_submitters.reshape(-1, 1), lambda a, s: jaro(a[0], s[0]))
closest_author = np.argmax(distances, axis=0)
print(distances)
print(all_submitters)
print(all_authors[closest_author])

all_names = np.unique(np.concatenate((all_authors, all_submitters))).reshape(-1, 1)
print(all_names)

distances = cdist(all_names, all_names, lambda a, s: jaro(a[0], s[0]))
print(distances.shape)
print(distances)

# Use a boolean mask to find rows where the value in the specified column is greater than 0.7
name_groups = [np.where(distances[:, i] > 0.7)[0] for i in range(len(all_names))]

visited = np.zeros(len(all_names), dtype=bool)
for i, group in enumerate(name_groups):
  if not visited[group[0]]:
    print(all_names[group])
    visited[group] = True
    # save
