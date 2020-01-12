def create_csv_from_item_list(headers, items):
    """
    Given a list of items, it generates the contents of a csv file from it and returns the data
    :param headers: The comma separated string representing header of the file, eg : a,b,c
    :param items: an array of items, where each item is a dictionary representing the a single row in the desired csv in correct order
    :return: csv content
    """

    file_body = headers + '\n'
    header_list = headers.split(',')
    for item in items:
        csv_item = ''
        for header in header_list:
            csv_item += str(item.get(header, '')) + ','
        csv_item = csv_item[:-1] + '\n'
        file_body = file_body + csv_item
    return file_body


def create_item_list_from_csv(csv_data):
    """
    Creates list of item-dict from csv-format (comma-separated) data
    :param csv_data: the data to be parsed
    :return: list of items
    """
    items = []

    csv_items = csv_data.split('\n')
    if len(csv_items) > 0:
        header = csv_items[0]
        attr = header.split(',')
        for each in csv_items[1:]:
            item_to_add = dict()
            for i, value in enumerate(each.split(',')):
                if len(value) <= i:
                    break
                item_to_add[attr[i]] = value
            if item_to_add:
                items.append(item_to_add)

    return items
