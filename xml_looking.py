import mwxml
import re

# def extract_interlanguage_links(text):
#     if text is None:
#         return []  # Return an empty list if text is None
#     interlanguage_link_pattern = r'\[\[([a-z]{2,3}|[a-z]{2,3}-[a-z]{2,3}):[^\]]+\]\]'
#     return re.findall(interlanguage_link_pattern, text)

def process_page(page):
    interlanguage_links = []
    for revision in page:
        # Check if revision text is not None
        if revision.text is not None:
            #interlanguage_links = extract_interlanguage_links(revision.text)
            revision=revision
            break  # Stop after processing the latest revision with text

    return {
        'id': page.id,
        'title': page.title,
        'namespace': page.namespace,
        #'interlanguage_links': interlanguage_links,
        'revision':[x for x in dir(revision) if not x[:2]==x[-2:]=='__'],
        #'text':revision.text,
    }



def process_dump(dump, path):
    for idx, page in enumerate(dump):
        # You can add a condition here to skip initial pages
        # For example, to start processing from the 100th page:
        if idx < 990:
            continue

        # Process each page using the process_page function
        yield process_page(page)

        #To limit the number of pages processed in each dump, break after a certain count
        if idx >= 1000:  # Adjust this number as per your requirement
            break

paths = ['data/hewiki-20240101-pages-articles.xml']
samples = list(enumerate(mwxml.map(process_dump, paths)))

print(paths)
print(samples)
