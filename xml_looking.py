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
        'revision':[x for x in dir(revision)], #if not x[:2]==x[-2:]=='__'],
        #'text':revision.text,
        'props':[x for x in dir(page) ]#if not x[:2]==x[-2:]=='__'],
    }

def parse_wikipedia_page(raw_text):
    # Split the text into lines
    lines = raw_text.split('\n')

    # Initialize variables
    sections = {}
    current_section_title = 'Summary'
    current_section_content = []

    # Regular expression for matching section headers
    section_header_pattern = re.compile(r"^(==+)([^=]+)\1$")

    for line in lines:
        # Check if the line is a section header
        match = section_header_pattern.match(line)
        if match:
            # Save the current section
            sections[current_section_title] = '\n'.join(current_section_content).strip()

            # Start a new section
            current_section_title = match.group(2).strip()
            current_section_content = []
        else:
            # Add line to the current section
            current_section_content.append(line)

    # Add the last section
    sections[current_section_title] = '\n'.join(current_section_content).strip()

    return sections



def process_dump(dump, path):
    for idx, page in enumerate(dump):
        # You can add a condition here to skip initial pages
        # For example, to start processing from the 100th page:
        if idx < 999:
            continue

        # Process each page using the process_page function
        yield process_page(page)

        #To limit the number of pages processed in each dump, break after a certain count
        if idx >= 1000:  # Adjust this number as per your requirement
            break

paths = ['data/hewiki-20240101-pages-articles.xml']
samples = list((mwxml.map(process_dump, paths)))

print(paths)
print(samples)
print(3*'\n')
#print([parse_wikipedia_page(x['text']) for x in samples])
