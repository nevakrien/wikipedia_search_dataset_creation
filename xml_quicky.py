import mwxml
import glob
from os.path import join 

paths = [join('data','hewiki-20240101-pages-articles.xml')]#glob.glob('wiki.xml')

# def process_dump(dump, path):
#   for page in dump:
#     for revision in page:
#         yield page.id, revision.id, revision.timestamp, len(revision.text)

# for page_id, rev_id, rev_timestamp, rev_textlength in mwxml.map(process_dump, paths):
#     print("\t".join(str(v) for v in [page_id, rev_id, rev_timestamp, rev_textlength]))

print(paths)
x=list(zip(range(10),mwxml.map(lambda dump,x: dump,paths)))
print(x)
print(4*"\n")
#print([(p[0],dir(p[1])) for p in x])
#print([p[1].__dict__.keys() for p in x])
#print([(p[0],p[1].title) for p in x])
print([(p[0],p[1].to_json()) for p in x])

#print(x[0].text)