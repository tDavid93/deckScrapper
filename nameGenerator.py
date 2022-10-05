import pandas as pd

#TODO: generate the content of lega_raw.txt

text_file = open('lega_raw.txt', 'r')
text = text_file.read()

#cleaning
text = text.lower()
words = text.split()
words = [word.strip('.,!;()[]') for word in words]
words = [word.replace("'s", '') for word in words]

#finding unique
unique = {}
for word in words:
 if word not in unique:
  unique[word] = 1
 else:
  unique[word] = unique[word] + 1
  

#sort
unique = pd.DataFrame(unique.items(), columns=['word', 'count'])
un_sort = unique.sort_values(by="count",ascending=False)
print(un_sort)
unique = {}

text_file = open('lega_raw.txt', 'r')
text = text_file.readlines()

for line in text:
  
  name = line.split(sep=',')
  name = name[0].split(sep='\n')
  name = name[0].lower()
  if name not in unique:
    unique[name] = 1
  else:
    unique[name] = unique[name] + 1

#finding unique

  

#sort
unique = pd.DataFrame(unique.items(), columns=['word', 'count'])



#print

un_name_sort = unique.sort_values(by="count",ascending=False)
print(un_name_sort)
wordlist = pd.concat((un_name_sort,un_sort))
unique_words = pd.unique(wordlist.word)
out = open("wordlist.txt", mode='w')


for name in unique_words:
 out.write(name + '\n')
out.close()