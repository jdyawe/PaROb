import yaml
'''
object: list of dict
every element is an object may be needed in the signal

Elements:
-type: 'Data' or 'ConfInterval'
-field: valid fields can be checked in the future
-userDefName: User Defined Name of this object, which can be directly access in the signal

signal: list of dict
every element is a signal that will appear in the Box

Elements:
-Components: components of the formula
-Formula: Evaluation of signal
-width, color, Move, Scale: plot settings
-NeedRename: 
-Rename:
-NeedConfInterval: Show Confidence Interval or not
'''

content = {
    r"510050": ["000016.SH", 900000]
    ,
    r"510300": ["000300.SH", 900000]
    ,
}

layout = {
    r"Position": (500, 500, 500, 500)
}





def yamlwritetst(filename, content):
    with open(filename,'w') as f:
        yaml.dump(content, f)

# def yamlloadtst(filename):
#     with open(filename,'r') as f:
#         lines = yaml.load(f, Loader=yaml.FullLoader)
#         for key, value in lines.items():
#             print(f"{key}:{value}")

if __name__=='__main__':
    filename = "configuration.yaml"
    yamlwritetst(filename, content)
    filename = 'layout.yaml'
    yamlwritetst(filename, layout)
