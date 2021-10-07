class Human:
    def __init__(self,h,w):
        self.h=h
        self.w=w
    def intro(self): 
        print(f"Weight of the person is {self.w}")
        print(f"Height of the person is {self.h}")
    def set_height(self,height): # Set Function
        self.h=height
    def set_weight(self,weight): # Set Function
        self.w=weight

class Male(Human):
    def __init__(self, name,h, w):
        super().__init__(h, w)
        self.name=name
    def intro(self):
        super().intro()
    def set_h(self,height):
        super().set_height(height)
    def set_w(self,weight):
        super().set_weight(weight)

# person=Human(180,65)
# person.intro()
male=Male("Yush",195,70)

male.intro()
male.set_h(200)
male.set_w(100)
# print(f"Weight of the Male is {male.w}")
print(f"Height Modified : {male.h}")
print(f"Weight Modified : {male.w}")
# person.set_height(190)
# person.set_weight(75)
# print(f"Weight of the person is {person.w}")
# print(f"Height of the person is {person.h}")