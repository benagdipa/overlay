input_string = input("Enter the strings separated by space: ")
string_list = input_string.split(',')
pows_lst = []
for string in string_list:
    pows_lst.append(string[-19:])
pows = "'" + """','""".join(pows_lst) + "'"
