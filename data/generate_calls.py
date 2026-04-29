#!/usr/bin/env python3
"""
Generate 500 synthetic patient call logs for GMR dispatcher QA demo.
Each call has 5-15 Q&A rows with realistic, varied dispatcher/caller exchanges.
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

# ── Output paths ──────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "calls")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "call_logs_batch_001.csv")
MAPPING_FILE = os.path.join(SCRIPT_DIR, "mappings", "protocol_mapping.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Load protocol mapping ────────────────────────────────────────────────────
legacy_to_protocol = {}
protocol_to_legacies = {}
with open(MAPPING_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        leg = row["legacy_protocol_name"]
        proto = row["new_protocol_name"]
        legacy_to_protocol[leg] = proto
        protocol_to_legacies.setdefault(proto, []).append(leg)

# ── Call distribution ─────────────────────────────────────────────────────────
PROTOCOL_COUNTS = [
    ("Chest Pain", 100),
    ("Breathing Problems", 100),
    ("Falls / Traumatic Injury", 75),
    ("Abdominal Pain", 50),
    ("Allergic Reaction / Anaphylaxis", 40),
    ("Seizures", 35),
    ("Overdose / Poisoning", 35),
    ("Stroke / Neurological Emergency", 25),
    ("Pregnancy Complications", 20),
    ("Unconscious / Unresponsive", 20),
]

# ── Fake name pools ──────────────────────────────────────────────────────────
FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Margaret",
    "Donald", "Sandra", "Steven", "Ashley", "Andrew", "Dorothy", "Paul",
    "Kimberly", "Joshua", "Emily", "Kenneth", "Donna", "Kevin", "Michelle",
    "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa", "Ronald",
    "Deborah", "Jason", "Stephanie", "Edward", "Rebecca", "Jeffrey", "Sharon",
    "Ryan", "Laura", "Jacob", "Cynthia", "Gary", "Kathleen", "Nicholas",
    "Amy", "Eric", "Angela", "Jonathan", "Shirley", "Stephen", "Anna",
    "Larry", "Brenda", "Justin", "Pamela", "Scott", "Emma", "Brandon",
    "Nicole", "Benjamin", "Helen", "Samuel", "Samantha", "Raymond", "Katherine",
    "Gregory", "Christine", "Frank", "Debra", "Alexander", "Rachel", "Patrick",
    "Carolyn", "Jack", "Janet", "Dennis", "Catherine", "Jerry", "Maria",
    "Tyler", "Heather", "Aaron", "Diane", "Jose", "Ruth", "Adam", "Julie",
    "Nathan", "Olivia", "Henry", "Joyce", "Douglas", "Virginia", "Peter",
    "Victoria", "Zachary", "Kelly", "Kyle", "Lauren", "Noah", "Christina",
    "Ethan", "Joan", "Marcus", "Evelyn", "Carlos", "Judith",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris",
    "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan",
    "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos",
    "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez",
    "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long",
    "Ross", "Foster", "Jimenez", "Powell",
]

STREETS = [
    "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Main", "Park",
    "Highland", "Sunset", "Lake", "River", "Meadow", "Forest", "Valley",
    "Mountain", "Prairie", "Spring", "Autumn", "Cherry", "Birch", "Walnut",
    "Hickory", "Aspen", "Willow", "Poplar", "Sycamore", "Magnolia",
    "Dogwood", "Juniper", "Lincoln", "Jefferson", "Franklin", "Adams",
    "Jackson", "Monroe", "Grant", "Harrison", "Madison",
]
STREET_TYPES = ["Street", "Avenue", "Drive", "Boulevard", "Lane", "Court",
                "Place", "Road", "Way", "Circle"]
CITIES = [
    ("Denver", "CO"), ("Colorado Springs", "CO"), ("Aurora", "CO"),
    ("Fort Collins", "CO"), ("Lakewood", "CO"), ("Thornton", "CO"),
    ("Arvada", "CO"), ("Pueblo", "CO"), ("Westminster", "CO"),
    ("Centennial", "CO"), ("Boulder", "CO"), ("Greeley", "CO"),
    ("Longmont", "CO"), ("Broomfield", "CO"), ("Castle Rock", "CO"),
    ("Parker", "CO"), ("Littleton", "CO"), ("Commerce City", "CO"),
    ("Northglenn", "CO"), ("Brighton", "CO"),
]

DISPATCH_TYPES = ["BLS", "ALS", "supervisor", "no_dispatch"]
OUTCOMES = ["transported_to_er", "treated_on_scene", "refusal",
            "phone_resolved", "no_patient_contact"]


def random_timestamp():
    """Random datetime between 2025-10-01 and 2026-03-26."""
    start = datetime(2025, 10, 1)
    end = datetime(2026, 3, 26)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_phone():
    return f"555-{random.randint(200,999)}-{random.randint(1000,9999)}"


def random_address():
    num = random.randint(100, 9999)
    street = random.choice(STREETS)
    stype = random.choice(STREET_TYPES)
    city, state = random.choice(CITIES)
    return f"{num} {street} {stype}, {city}, {state}"


# ── Question/answer templates per complaint type ─────────────────────────────
# Each entry: (dispatcher_question, [list of possible caller answers])
# We build varied realistic exchanges.

COMMON_OPENING = [
    ("911, what is the address of your emergency?",
     [
         "It's {addr}",
         "We're at {addr}",
         "{addr}, please hurry",
         "Um, {addr}... yeah, {addr}",
         "I'm at {addr}, we need help right away",
     ]),
    ("What is a callback number?",
     [
         "{phone}",
         "You can reach me at {phone}",
         "My number is {phone}",
         "{phone}, that's my cell",
     ]),
    ("Tell me exactly what happened.",
     None),  # Filled per complaint type
    ("How old is the patient?",
     None),  # Filled dynamically
]

# Per-complaint-type question banks
# Each list has (question, [answers]) tuples.  We'll sample from these.

CHEST_PAIN_QA = [
    ("Tell me exactly what happened.",
     [
         "My husband is having really bad chest pain, it just started maybe 20 minutes ago",
         "I'm having chest pain, it feels like an elephant sitting on my chest",
         "My dad is complaining of chest tightness and he looks really pale",
         "She grabbed her chest and said it really hurts, she's been sweating a lot",
         "I've got this pressure in my chest that won't go away, started at work",
         "My mom called me saying her chest hurts, she sounds real scared",
         "He was just sitting watching TV and grabbed his chest, says it's burning",
         "I woke up with this pain in my chest, it's getting worse",
         "My wife is having chest pain and she can barely breathe",
         "He's been complaining about his chest hurting for like an hour now",
     ]),
    ("How old is the patient?",
     ["He's 62", "She's 74", "I'm 55", "He's about 58", "She's 81",
      "I'm 47", "He's 69", "She's 66", "I think 72", "He's 50"]),
    ("Is the patient conscious?",
     ["Yeah he's awake", "Yes she's conscious but real uncomfortable",
      "Yes, he's sitting up talking to me", "Yeah but she looks real bad",
      "He's awake but kind of out of it", "Yes, she's alert"]),
    ("Is the patient breathing?",
     ["Yes but it's kind of labored", "Yeah he's breathing",
      "She's breathing fast", "Yes but she says it hurts to breathe",
      "He's breathing okay I think", "Yeah, short of breath though"]),
    ("Where exactly is the pain?",
     ["Right in the center of his chest", "She says it's on the left side",
      "He's pointing to right here in the middle",
      "She says it's like behind her breastbone",
      "All across the front of his chest",
      "He says it's kind of everywhere in his chest"]),
    ("Does the pain spread to the arm, jaw, neck, or back?",
     ["Yeah it's going down his left arm", "She says her jaw hurts too",
      "He says it goes into his back a little",
      "No it's just in the chest",
      "Yeah, his left arm is numb",
      "She says her neck feels tight but that might be something else"]),
    ("How would you describe the pain -- is it crushing, squeezing, or pressure-like?",
     ["He says it's like a squeezing feeling", "She says it's pressure, heavy pressure",
      "It's crushing, he can barely talk", "He says it's a burning sensation actually",
      "She describes it as tight, like a band around her chest",
      "He says it's sharp, like stabbing"]),
    ("On a scale of 1 to 10, how severe is the pain?",
     ["He says it's a 9", "She says 8", "Like a 7 I guess",
      "He says 10, it's the worst pain ever", "She says maybe a 6",
      "He's saying it's about a 5 now, was worse before"]),
    ("When did the pain start?",
     ["Maybe 20 minutes ago", "About an hour ago",
      "It started maybe 30 minutes ago", "I don't know, maybe like 45 minutes?",
      "It just started like 10 minutes ago", "He says it's been going on for 2 hours"]),
    ("Has the pain been constant or does it come and go?",
     ["It's been constant since it started", "It comes and goes but mostly it's there",
      "It was coming and going but now it's constant",
      "Constant, hasn't let up at all"]),
    ("Is the patient sweating more than normal or feeling clammy?",
     ["Yeah he's real pale and sweaty", "She's soaking wet, her shirt is drenched",
      "He looks clammy, kind of gray", "A little sweaty but not too bad",
      "No, not really", "Yeah she's sweating bullets"]),
    ("Does the patient feel nauseous or has the patient vomited?",
     ["He threw up once already", "She says she feels like she's gonna puke",
      "No nausea", "He's nauseous but hasn't vomited",
      "She vomited twice", "He feels a little queasy"]),
    ("Is the patient short of breath?",
     ["Yeah, can't hardly breathe", "A little bit",
      "She's huffing and puffing", "No, breathing seems okay",
      "He's having trouble catching his breath", "Mild shortness of breath"]),
    ("Has the patient had a heart attack before?",
     ["Yeah he had one three years ago", "No, never",
      "She had bypass surgery two years ago", "Not that I know of",
      "He's had two heart attacks before", "No, this is the first time"]),
    ("Does the patient take any heart medications?",
     ["He takes nitroglycerin", "She takes a bunch of heart pills, I don't know the names",
      "Yeah, he's on metoprolol and aspirin", "No medications",
      "She has a nitroglycerin spray", "I'm not sure about his medications"]),
    ("Does the patient take blood thinners or aspirin?",
     ["He takes aspirin every day", "She's on Eliquis",
      "Yeah, baby aspirin", "No blood thinners",
      "He takes warfarin", "I don't think so"]),
    ("What was the patient doing when the pain started?",
     ["He was just sitting on the couch", "She was walking up the stairs",
      "He was shoveling snow", "Just eating dinner",
      "She was sleeping and it woke her up", "He was at work, lifting boxes"]),
]

BREATHING_QA = [
    ("Tell me exactly what happened.",
     [
         "My mom can't breathe, she's wheezing really bad",
         "He's having a really hard time breathing, it came on suddenly",
         "She's gasping for air, I think it's her asthma",
         "My husband woke up and can't catch his breath",
         "The baby is breathing really fast and making weird sounds",
         "He was eating and started choking and now he can't breathe right",
         "She's been getting worse all day, now she can't hardly breathe",
         "My dad is on oxygen and his breathing is getting worse",
         "I can't breathe, my chest is so tight",
         "She's wheezing so bad you can hear it across the room",
     ]),
    ("How old is the patient?",
     ["She's 68", "He's 72", "She's 45", "He's 3 months old",
      "I'm 34", "He's 78", "She's 56", "He's 82",
      "She's about 60", "He's 41"]),
    ("Is the patient conscious?",
     ["Yes but she's struggling", "Yeah he's awake",
      "She's conscious but panicking", "Yes, he's sitting up",
      "Yes but she looks exhausted", "He's awake but barely"]),
    ("Is the patient breathing?",
     ["Yes but it's really labored", "Barely, it's really shallow",
      "Yes but you can hear the wheezing from here",
      "She's breathing really fast", "He's breathing but it sounds terrible",
      "Yes, but she has to sit up to breathe at all"]),
    ("Can the patient speak in full sentences?",
     ["No, just a few words at a time", "She can barely get a word out",
      "He's talking but has to stop and gasp between words",
      "Yeah she can talk but she's winded",
      "No, maybe two or three words then he has to stop",
      "She can talk in short sentences"]),
    ("Do you hear any wheezing or whistling when the patient breathes?",
     ["Yeah, really loud wheezing", "It's like a whistling sound when she breathes out",
      "His chest sounds real rattly", "No wheezing, just fast breathing",
      "Yeah there's a definite wheeze", "I can hear something but I'm not sure what"]),
    ("Are the lips or fingertips turning blue or gray?",
     ["Oh God, yeah his lips look bluish", "Her fingernails are kind of gray",
      "No, color looks okay", "I can't tell, maybe a little blue around the lips",
      "Yeah, definitely blue", "No, she looks pale but not blue"]),
    ("When did the breathing difficulty start?",
     ["About 30 minutes ago", "It's been getting worse all day",
      "Maybe an hour ago", "Just a few minutes ago, it was sudden",
      "She's been struggling since last night", "He woke up like this about 20 minutes ago"]),
    ("Did it come on suddenly or gradually?",
     ["It was sudden, out of nowhere", "It's been getting gradually worse",
      "Kind of gradual, then got real bad all at once",
      "Sudden, she was fine 10 minutes ago",
      "It's been building all day"]),
    ("Does the patient have asthma or COPD?",
     ["Yeah she has COPD, been on inhalers for years",
      "He has asthma since he was a kid",
      "She's got emphysema", "No, nothing like that",
      "He has chronic bronchitis", "She's got severe asthma"]),
    ("Does the patient have an inhaler? Has the patient used it?",
     ["Yeah she used it twice already, it's not helping",
      "He's been hitting the inhaler every 20 minutes, nothing's working",
      "She used it once and it helped a little",
      "He doesn't have it with him, it's at home",
      "No inhaler", "Yes, she just used it but it didn't do much"]),
    ("Is there any swelling of the face, tongue, or throat?",
     ["No swelling", "Her face looks puffy actually",
      "His throat seems swollen", "I don't think so",
      "Yeah her lips are swelling up", "No, nothing like that"]),
    ("Does the patient have a fever or cough?",
     ["She's been coughing all week", "He feels hot, probably has a fever",
      "Yeah, bad cough with green stuff", "No fever, just the breathing",
      "She's had a cold for a few days", "Coughing a lot but no fever"]),
    ("Is the patient on home oxygen?",
     ["Yes, 2 liters", "No", "Yeah, she's on 3 liters usually",
      "He has a concentrator at home but it's not working right",
      "No, she's never been on oxygen", "Yes, 4 liters continuous"]),
    ("Does the patient have any chest pain?",
     ["No chest pain", "She says her chest is tight but no real pain",
      "Yeah he's got chest pain too", "A little, from the coughing",
      "No pain, just can't breathe", "She says it hurts when she tries to take a deep breath"]),
]

FALLS_QA = [
    ("Tell me exactly what happened.",
     [
         "My grandma fell down the stairs, she can't get up",
         "He fell off a ladder, maybe like 8 feet up",
         "She tripped on the sidewalk and hit her head",
         "My dad fell in the bathroom, he's on the floor",
         "He fell off his bike and his arm looks really bad",
         "She slipped on ice and she's complaining about her hip",
         "He fell at work from a scaffolding, he's conscious but in a lot of pain",
         "My mom fell out of bed and she's on blood thinners",
         "He was walking and just collapsed, fell straight forward",
         "She fell down about 3 stairs and landed on her wrist",
     ]),
    ("How old is the patient?",
     ["She's 84", "He's 67", "He's 42", "She's 78",
      "He's 35", "She's 91", "He's 55", "She's 72",
      "He's 28", "She's 65"]),
    ("Is the patient conscious?",
     ["Yes but she's in a lot of pain", "He's conscious but dazed",
      "Yeah she's awake and talking", "He blacked out for a second but he's back now",
      "Yes, he's alert", "She's going in and out"]),
    ("Is the patient breathing normally?",
     ["Yes, breathing is fine", "She's breathing fast, probably from the pain",
      "Yeah, seems normal", "He's breathing okay",
      "A little rapid but okay", "Yes"]),
    ("How far did the patient fall?",
     ["Down about 6 stairs", "Off a 10-foot ladder",
      "Just from standing, she tripped", "Off the porch, maybe 3 feet",
      "Off a roof, two stories", "He fell from standing height"]),
    ("What part of the body hit first?",
     ["She hit her head on the ground", "He landed on his side",
      "She caught herself with her hands", "He landed on his back",
      "Her hip hit the ground first", "I'm not sure, it happened so fast"]),
    ("Did the patient hit their head?",
     ["Yeah she hit her head pretty hard", "No, he didn't hit his head",
      "I think so, there's a bump forming", "He's got a cut on his forehead",
      "She says she didn't but I'm not sure", "Yeah, his head bounced off the concrete"]),
    ("Did the patient lose consciousness at any point?",
     ["Yeah for just a second", "No, she was awake the whole time",
      "I think he blacked out briefly", "No, he never lost consciousness",
      "I'm not sure, I wasn't watching", "She says she didn't"]),
    ("Is there any bleeding?",
     ["Yeah there's a lot of blood from a cut on her head",
      "A little scrape on his elbow, not much",
      "There's blood coming from his nose",
      "No bleeding that I can see",
      "She's got a gash on her shin, bleeding pretty good",
      "His knee is bleeding but it's not too bad"]),
    ("Can the patient move all arms and legs?",
     ["She can't move her right arm", "He can move everything but it hurts",
      "Yeah, she's moving everything", "He can't feel his legs",
      "She won't move her left wrist, says it hurts too much",
      "Everything moves but the left ankle"]),
    ("Is there pain in the neck or back?",
     ["She says her back hurts where she landed",
      "No neck or back pain", "He's complaining about his neck",
      "Yeah her lower back is killing her",
      "He says his neck is sore", "No, just the leg"]),
    ("Is there any obvious deformity?",
     ["Her wrist looks bent wrong", "His ankle is all swollen and turned",
      "No, nothing looks deformed", "His arm is definitely broken, you can see it",
      "Her leg looks kind of off", "No deformity that I can see"]),
    ("Is the patient on blood thinners?",
     ["Yes, she takes Eliquis", "He's on warfarin",
      "No blood thinners", "I think she takes something but I don't know the name",
      "Yes, Xarelto", "No"]),
    ("Did the patient feel dizzy or faint before the fall?",
     ["She said she got dizzy and then fell", "No he just tripped",
      "He said the room was spinning", "She says she felt fine before",
      "He might have blacked out first", "No, she just slipped on the wet floor"]),
]

ABDOMINAL_PAIN_QA = [
    ("Tell me exactly what happened.",
     [
         "My stomach hurts so bad I can't stand up straight",
         "She's been having terrible belly pain since this morning",
         "He's doubled over in pain, clutching his stomach",
         "I've been vomiting all day and the pain is getting worse",
         "My daughter has really bad stomach cramps",
         "He says the pain is in his right side, it's really sharp",
         "She woke up with this horrible pain in her lower belly",
         "The pain started after dinner and it's getting unbearable",
     ]),
    ("How old is the patient?",
     ["I'm 34", "She's 52", "He's 67", "She's 28",
      "He's 45", "I'm 71", "She's 19", "He's 58"]),
    ("Is the patient conscious?",
     ["Yes", "Yeah, awake but miserable", "Yes, she's conscious",
      "He's awake but barely", "Yes, just in a lot of pain"]),
    ("Where exactly is the pain?",
     ["Right side, low down", "All over my belly", "Upper left",
      "Right under my belly button", "It started around my navel then moved to the right side",
      "Lower left", "Up high, near my ribs on the right"]),
    ("When did the pain start?",
     ["This morning around 6 AM", "A few hours ago",
      "Yesterday but it got way worse tonight", "About an hour ago",
      "It's been on and off for a couple days, but now it's constant"]),
    ("Has the patient vomited?",
     ["Yeah, three times already", "She's been dry heaving",
      "He threw up once", "No vomiting", "I can't keep anything down",
      "Yes, and there was some blood in it"]),
    ("Does the patient have diarrhea?",
     ["Yes, really bad", "A little", "No", "Yeah, it's been watery all day",
      "Some, not too bad"]),
    ("Is there any fever?",
     ["She feels really hot", "I don't have a thermometer but I feel feverish",
      "No fever", "He's been running a temp all day",
      "She's warm to the touch"]),
    ("Is the abdomen rigid or hard to the touch?",
     ["Don't touch it! It hurts when anything touches it",
      "It's really tender on the right side",
      "It feels kind of hard and bloated",
      "She won't let me near it", "It's soft but tender"]),
    ("Has the patient had any surgeries?",
     ["He had his appendix out years ago", "No surgeries",
      "She had a C-section last year", "He had hernia surgery",
      "No, never had surgery"]),
    ("Is there any blood in the stool or urine?",
     ["I noticed some blood when I went to the bathroom",
      "No blood", "She says her urine is dark",
      "Yeah there was some blood in the stool",
      "I haven't checked"]),
    ("Is the patient pregnant or could the patient be pregnant?",
     ["No, definitely not", "I don't think so",
      "She says no", "That's not possible, he's male",
      "She might be, she's not sure"]),
]

ALLERGIC_REACTION_QA = [
    ("Tell me exactly what happened.",
     [
         "She ate some shrimp and now her throat is swelling up",
         "He got stung by a bee and he's breaking out in hives everywhere",
         "My son ate something with peanuts and his face is all swollen",
         "She took a new medication and now she's got a rash all over",
         "He's having an allergic reaction, his lips are swelling",
         "I ate something and my tongue feels thick, I can't swallow right",
         "She's covered in hives and she's starting to wheeze",
         "His eyes are swollen shut and he's itching everywhere",
     ]),
    ("How old is the patient?",
     ["She's 32", "He's 8", "He's 45", "She's 27",
      "He's 14", "I'm 38", "She's 56", "He's 22"]),
    ("Is the patient conscious?",
     ["Yes but really scared", "Yeah he's awake",
      "She's conscious, getting anxious", "Yes"]),
    ("Is the patient breathing?",
     ["Yes but it sounds wheezy", "He's breathing okay for now",
      "She's having trouble breathing", "Breathing fine, just covered in hives",
      "He's starting to wheeze"]),
    ("Is there any swelling of the face, lips, tongue, or throat?",
     ["Yeah her lips and tongue are swelling", "His face is puffy",
      "No swelling, just hives", "His throat feels tight he says",
      "Her eyes are swollen", "The tongue looks swollen"]),
    ("Is the patient having any difficulty swallowing?",
     ["She says she can't swallow right", "No swallowing problems",
      "He says his throat feels funny", "Yeah, she's drooling because she can't swallow",
      "A little bit of difficulty"]),
    ("Does the patient have a known allergy?",
     ["Yeah, she's allergic to shellfish", "He's allergic to bee stings",
      "Peanut allergy", "We didn't know about any allergies",
      "She's allergic to penicillin, she just took amoxicillin by mistake",
      "He has multiple food allergies"]),
    ("Does the patient have an EpiPen?",
     ["Yes! Where is it... hold on... I found it",
      "No, we don't have one", "It's expired, is that okay?",
      "Yes, should I use it?", "He left it at home",
      "Yeah she has one in her purse"]),
    ("Has the patient used the EpiPen?",
     ["Yes, I just gave it to her", "No, I'm afraid to use it",
      "Not yet, should I?", "He used it but it doesn't seem to be working",
      "N/A, no EpiPen"]),
    ("How long ago was the patient exposed to the allergen?",
     ["About 15 minutes ago", "Maybe half an hour",
      "She ate it about 10 minutes ago", "The sting was maybe 5 minutes ago",
      "About an hour ago", "Just a few minutes ago"]),
    ("Is the patient dizzy or lightheaded?",
     ["She's getting dizzy", "He says he feels faint",
      "No dizziness", "A little lightheaded",
      "She's really woozy"]),
    ("Any hives or rash? Where?",
     ["Hives all over his chest and arms", "Yeah, big red welts everywhere",
      "Just on her face and neck", "Hives spreading from his stomach to his back",
      "Red blotchy rash on her arms", "Covered head to toe"]),
]

SEIZURE_QA = [
    ("Tell me exactly what happened.",
     [
         "He's having a seizure right now, he's shaking all over",
         "She just had a seizure, it lasted about two minutes, she's not responding yet",
         "My son is convulsing on the floor",
         "She collapsed and started jerking, I think it's a seizure",
         "He's epileptic and he's having a bad one right now",
         "She was talking and then her eyes rolled back and she started shaking",
         "He seized up about 5 minutes ago and he's still out of it",
         "My daughter is having convulsions, this has never happened before",
     ]),
    ("How old is the patient?",
     ["He's 24", "She's 4", "He's 55", "She's 17",
      "He's 38", "She's 67", "He's 2", "She's 45"]),
    ("Is the patient still seizing?",
     ["Yes, he's still shaking", "No, it stopped but she's not waking up",
      "It just stopped I think", "Yes, it won't stop",
      "He's twitching on one side", "No, she stopped a minute ago"]),
    ("How long has the seizure lasted?",
     ["Maybe 2 minutes", "It's been going for about 5 minutes now",
      "It lasted about a minute", "I don't know, it feels like forever, maybe 3 minutes",
      "It was about 30 seconds", "Over 5 minutes for sure"]),
    ("Is the patient breathing?",
     ["I can't tell", "I think so, her chest is moving",
      "He's making these grunting sounds", "Yes, she's breathing",
      "His lips are turning blue", "He started breathing again after it stopped"]),
    ("Is there any blood coming from the mouth?",
     ["Yeah, I think he bit his tongue", "A little blood",
      "No blood", "There's some blood and foam",
      "She's bleeding from her lip"]),
    ("Does the patient have a seizure disorder or epilepsy?",
     ["Yes, he's had epilepsy since he was a kid",
      "No, this is the first time",
      "She has seizures but they're usually controlled",
      "He was diagnosed a few years ago",
      "Not that I know of", "Yes, she has a history"]),
    ("Is the patient on seizure medication?",
     ["Yeah, Keppra", "He takes something but I don't know the name",
      "She's on Depakote", "No medications",
      "He's supposed to be but I think he stopped taking it",
      "Yes, multiple medications"]),
    ("Did the patient hit their head when they fell?",
     ["I think so, he went down hard", "I caught her before she hit",
      "Yeah, his head hit the tile floor", "I'm not sure",
      "No, she was already on the couch", "He definitely hit his head"]),
    ("Has the patient had a fever today?",
     ["She's been sick with a fever all day", "No fever",
      "He felt warm earlier", "I don't know",
      "Yeah, 102 this morning", "No, she seemed fine before this"]),
    ("Did anything happen before the seizure?",
     ["He said he felt weird, like an aura thing", "Nothing, it was out of nowhere",
      "She was staring off and wouldn't respond, then started seizing",
      "He was complaining of a headache", "She just woke up from a nap",
      "He missed his medication yesterday"]),
]

OVERDOSE_QA = [
    ("Tell me exactly what happened.",
     [
         "I think he took a bunch of pills, there's an empty bottle next to him",
         "She overdosed on something, she's barely conscious",
         "My friend shot up heroin and now he's not responding",
         "I found my teenager with empty pill bottles",
         "He took too much of his pain medication",
         "She drank a whole bottle of something, I don't know what it was",
         "My roommate mixed pills and alcohol, he's passing out",
         "He admitted he took a bunch of Xanax and Percocet",
     ]),
    ("How old is the patient?",
     ["He's 23", "She's 31", "He's 17", "She's 45",
      "He's 28", "She's 19", "He's 52", "She's 37"]),
    ("Is the patient conscious?",
     ["Barely, he's in and out", "She's not responding to me",
      "He's conscious but really drowsy", "No, she's completely out",
      "He's awake but not making any sense", "She's groaning but won't open her eyes"]),
    ("Is the patient breathing?",
     ["Yeah but it's really slow", "Barely, like a breath every 10 seconds",
      "I can see his chest moving a little", "She's breathing but it's shallow",
      "I think so... yes, she's breathing", "He's snoring really loud"]),
    ("Do you know what the patient took?",
     ["I think it was Tylenol, there's an empty bottle",
      "Heroin, there's a needle right here",
      "A bunch of his Oxycodone", "I found empty bottles of Xanax and Ambien",
      "She drank drain cleaner", "I have no idea, there are empty bottles everywhere",
      "Fentanyl I think, from the street",
      "A bunch of sleeping pills"]),
    ("How much did the patient take?",
     ["The whole bottle was full yesterday, now it's empty, that's like 30 pills",
      "I don't know, maybe a handful", "She said she took about 20",
      "I have no idea", "Most of the bottle, I think there were 60 in there",
      "He says he only took a few extra but I don't believe him"]),
    ("When did the patient take it?",
     ["I just found him like this, I don't know when",
      "She told me about 30 minutes ago", "Maybe an hour ago",
      "I found the bottles, it could have been hours ago",
      "He called me 20 minutes ago saying he took something",
      "I think it was pretty recent, within the last hour"]),
    ("Was this intentional or accidental?",
     ["I think it was on purpose, she's been really depressed",
      "Accidental, he didn't know how much he was taking",
      "I don't know", "He was trying to get high",
      "She left a note... yes, it was intentional",
      "Accidental, she mixed up her medications"]),
    ("Has the patient vomited?",
     ["Yeah, he threw up once", "No vomiting",
      "She's been throwing up", "He vomited what looks like pills",
      "Not yet", "Yes, multiple times"]),
    ("Do you have Narcan available?",
     ["What's that?", "No", "Yes! I have it, should I give it?",
      "I already gave him one dose but nothing happened",
      "Yeah, I have the nasal spray", "I gave her Narcan 5 minutes ago and she's starting to come around"]),
    ("Are the patient's pupils very small or very large?",
     ["His pupils are tiny, like pinpoints", "I can't tell",
      "Her eyes are dilated huge", "They look really small",
      "I don't want to pry his eyes open", "Normal I think"]),
]

STROKE_QA = [
    ("Tell me exactly what happened.",
     [
         "My wife's face is drooping on one side and she can't talk right",
         "He suddenly can't move his right arm",
         "She started slurring her words out of nowhere",
         "My dad can't walk all of a sudden, his left side isn't working",
         "She said she has the worst headache of her life and now she's confused",
         "He was fine one minute and the next he couldn't speak",
         "Her face looks lopsided and she keeps saying weird things",
     ]),
    ("How old is the patient?",
     ["She's 71", "He's 65", "She's 58", "He's 80",
      "She's 74", "He's 69", "She's 62"]),
    ("Is the patient conscious?",
     ["Yes but confused", "He's awake but not making sense",
      "She's conscious", "He's getting more and more drowsy",
      "Yes, she knows who I am but can't say the words right"]),
    ("Can the patient speak? Are the words clear?",
     ["She's trying to talk but it's all garbled", "He can't get any words out",
      "Her speech is slurred, like she's drunk but she hasn't been drinking",
      "He can say a few words but they're wrong words",
      "She keeps saying the same word over and over",
      "He's making sounds but can't form words"]),
    ("Can the patient smile? Is the face symmetric?",
     ["One side of her face is drooping", "The left side of his face won't move",
      "Yeah, his mouth is sagging on the right",
      "Her smile is lopsided", "I can't tell, she won't try"]),
    ("Can the patient raise both arms and hold them up?",
     ["Her left arm just drops right down", "He can't lift his right arm at all",
      "One arm drifts down when she tries", "He can raise both but the right is weak",
      "She can barely lift the left one", "His right arm is completely limp"]),
    ("When did the symptoms start?",
     ["About 30 minutes ago", "I noticed it maybe 20 minutes ago",
      "She was fine at lunch, it's been about 2 hours",
      "It just happened, like 10 minutes ago",
      "I'm not sure, I found her like this when I got home",
      "About an hour ago"]),
    ("When was the patient last known to be normal?",
     ["She was fine at 2 o'clock, it's been about 3 hours",
      "He was normal when he went to bed last night",
      "About an hour ago she was talking fine",
      "Maybe 45 minutes ago",
      "I talked to him on the phone this morning and he was fine"]),
    ("Does the patient have any weakness or numbness?",
     ["The whole left side seems weak", "His right hand is numb",
      "She can't feel her left leg",
      "He's having trouble gripping things with his right hand",
      "Weakness in both legs", "Left arm and leg are weak"]),
    ("Does the patient have a history of stroke or TIA?",
     ["She had a mini-stroke last year", "No, never",
      "He had a stroke 5 years ago", "Yes, she's had two TIAs",
      "Not that I know of", "He had a stroke before on the same side"]),
    ("Does the patient have high blood pressure?",
     ["Yes, she's on blood pressure medication",
      "Yeah, he takes lisinopril", "I think so",
      "Yes, but she doesn't always take her pills",
      "No", "He's been treated for it"]),
    ("Is the patient on blood thinners?",
     ["Yes, Coumadin", "She takes aspirin every day",
      "No blood thinners", "He's on Eliquis",
      "I think she takes something for that"]),
    ("Does the patient have a severe headache?",
     ["She says it's the worst headache she's ever had",
      "He's not complaining about a headache",
      "Yeah, bad headache", "She can't communicate well enough to tell me",
      "No headache"]),
]

PREGNANCY_QA = [
    ("Tell me exactly what happened.",
     [
         "She's pregnant and she's bleeding really bad",
         "My wife is having contractions and they're coming really fast",
         "She thinks she's in labor, she's only 7 months along",
         "I'm pregnant and I'm having really bad cramps and bleeding",
         "She fell and she's 8 months pregnant, now she's having pain",
         "My daughter is pregnant and she's having really bad headaches and swelling",
         "She's been bleeding since this morning and she's about 6 months along",
     ]),
    ("How old is the patient?",
     ["She's 28", "She's 34", "I'm 22", "She's 31",
      "She's 19", "She's 37", "She's 26"]),
    ("How many weeks pregnant is the patient?",
     ["About 32 weeks", "She's 38 weeks", "I think she's 28 weeks",
      "Full term, 39 weeks", "About 7 months",
      "She's 24 weeks", "36 weeks"]),
    ("Is the patient conscious?",
     ["Yes", "Yeah she's awake", "She's conscious but in a lot of pain",
      "Yes, she's alert", "She's getting a little lightheaded"]),
    ("Is there any bleeding? How much?",
     ["Yeah, she's soaking through pads", "A little spotting",
      "There's a lot of blood", "Some bleeding, maybe like a period amount",
      "She's hemorrhaging, it's everywhere", "Just some light bleeding"]),
    ("Is the patient having contractions?",
     ["Yes, every 2 minutes", "She's having some cramping",
      "Yeah, they're coming fast", "No contractions, just bleeding",
      "They're about 5 minutes apart", "She says it's constant pain, not contractions"]),
    ("Has her water broken?",
     ["Yes, about an hour ago", "I think so, there's fluid",
      "No, I don't think so", "Yeah it just broke",
      "She's not sure", "Yes, and the fluid looks greenish"]),
    ("Can the patient feel the baby moving?",
     ["She says the baby's been moving a lot",
      "She hasn't felt the baby move in a while",
      "Yes, baby is active", "She's not sure",
      "The baby is kicking like crazy",
      "She says she hasn't felt movement today"]),
    ("Does the patient have any swelling in the face or hands?",
     ["Yeah, her face is really puffy", "Her hands and feet are swollen",
      "No unusual swelling", "Her ankles are huge",
      "Some swelling in her hands"]),
    ("Does the patient have a severe headache or vision changes?",
     ["She says her head is pounding", "She's seeing spots",
      "No headache or vision issues", "Yeah, bad headache and blurry vision",
      "She's had a headache all day"]),
    ("Is this the patient's first pregnancy?",
     ["No, she has two other kids", "Yes, first pregnancy",
      "Third pregnancy", "First one", "She has one other child"]),
    ("Has the patient had any complications during this pregnancy?",
     ["She was diagnosed with preeclampsia at her last appointment",
      "No, everything has been normal", "She has gestational diabetes",
      "Placenta previa, they told her", "No complications",
      "She's been on bed rest for a few weeks"]),
]

UNCONSCIOUS_QA = [
    ("Tell me exactly what happened.",
     [
         "I found him on the floor, he's not responding",
         "She just collapsed, I can't wake her up",
         "My husband won't wake up, I've been trying for 10 minutes",
         "He passed out and I can't get him to come to",
         "I came home and found my mom on the floor unconscious",
         "She fainted and she's not coming around",
         "He's been unresponsive since I got here, I don't know how long",
     ]),
    ("How old is the patient?",
     ["He's 75", "She's 60", "He's 48", "She's 82",
      "He's 55", "She's 70", "He's 39"]),
    ("Is the patient breathing?",
     ["I think so... yes, he's breathing", "Barely, it's really shallow",
      "She's not breathing! No wait... yes, very slowly",
      "He's making these gasping sounds", "Yes, she's breathing normally",
      "I can't tell, let me check... yes, he's breathing"]),
    ("Can you feel a pulse?",
     ["I think so, it's weak", "Let me try... yeah I feel something in the neck",
      "I can't find one... wait, yes, there it is",
      "His pulse is really fast", "She has a pulse, it's slow",
      "I can feel it, it's faint"]),
    ("Is there any response when you tap the shoulders and shout?",
     ["Nothing at all", "He groaned a little",
      "No response", "She moved her hand slightly",
      "Nothing, completely unresponsive",
      "He made a sound but won't open his eyes"]),
    ("Do you know what happened before the patient became unresponsive?",
     ["He was complaining of a headache and then just went down",
      "She was eating dinner and slumped over",
      "I have no idea, I just found him like this",
      "She said she felt dizzy and then passed out",
      "He took his insulin and then this happened",
      "She was fine 30 minutes ago when I talked to her on the phone"]),
    ("Does the patient have any medical conditions?",
     ["He's diabetic", "She has heart problems",
      "He's got diabetes and high blood pressure",
      "I'm not sure about her medical history",
      "He has a seizure disorder",
      "She takes a lot of medications but I don't know what for"]),
    ("Is the patient on any medications?",
     ["He takes insulin", "She's got a whole bunch of pill bottles",
      "Blood pressure and diabetes meds", "I don't know",
      "He takes heart medication", "A lot but I'm not sure which ones"]),
    ("Are there any signs of injury or bleeding?",
     ["No, no injuries", "There's a bump on her head, she might have fallen",
      "He's got blood on his forehead",
      "No visible injuries", "She might have hit her head going down",
      "I don't see any blood or anything"]),
    ("Is the skin color normal?",
     ["He's really pale", "She looks gray",
      "His face is flushed, really red", "She looks okay color-wise",
      "He's kind of blue around the lips", "She's very pale"]),
    ("Is there any vomit near the patient?",
     ["Yeah, there's vomit on the floor", "No",
      "He threw up earlier", "There's some, I'm going to turn her on her side",
      "No vomit", "Yeah, I need to clear his mouth"]),
]

QA_BANKS = {
    "Chest Pain": CHEST_PAIN_QA,
    "Breathing Problems": BREATHING_QA,
    "Falls / Traumatic Injury": FALLS_QA,
    "Abdominal Pain": ABDOMINAL_PAIN_QA,
    "Allergic Reaction / Anaphylaxis": ALLERGIC_REACTION_QA,
    "Seizures": SEIZURE_QA,
    "Overdose / Poisoning": OVERDOSE_QA,
    "Stroke / Neurological Emergency": STROKE_QA,
    "Pregnancy Complications": PREGNANCY_QA,
    "Unconscious / Unresponsive": UNCONSCIOUS_QA,
}

# ── Dispatch weighting by complaint type ──────────────────────────────────────
# (BLS, ALS, supervisor, no_dispatch) weights
DISPATCH_WEIGHTS = {
    "Chest Pain": [15, 65, 10, 10],
    "Breathing Problems": [20, 55, 15, 10],
    "Falls / Traumatic Injury": [40, 35, 10, 15],
    "Abdominal Pain": [45, 30, 10, 15],
    "Allergic Reaction / Anaphylaxis": [35, 40, 10, 15],
    "Seizures": [20, 55, 15, 10],
    "Overdose / Poisoning": [15, 55, 20, 10],
    "Stroke / Neurological Emergency": [10, 70, 10, 10],
    "Pregnancy Complications": [20, 55, 15, 10],
    "Unconscious / Unresponsive": [5, 75, 15, 5],
}

OUTCOME_WEIGHTS = {
    "Chest Pain": [60, 10, 10, 10, 10],
    "Breathing Problems": [55, 15, 10, 10, 10],
    "Falls / Traumatic Injury": [40, 25, 15, 5, 15],
    "Abdominal Pain": [45, 15, 15, 15, 10],
    "Allergic Reaction / Anaphylaxis": [35, 25, 15, 15, 10],
    "Seizures": [55, 20, 10, 5, 10],
    "Overdose / Poisoning": [55, 15, 5, 10, 15],
    "Stroke / Neurological Emergency": [65, 10, 5, 10, 10],
    "Pregnancy Complications": [50, 20, 10, 10, 10],
    "Unconscious / Unresponsive": [60, 10, 5, 10, 15],
}


def pick_weighted(options, weights):
    return random.choices(options, weights=weights, k=1)[0]


# ── Ambiguous / Insufficient call modifiers ──────────────────────────────────
AMBIGUOUS_ANSWERS = [
    "Well, I'm not really sure... it kind of hurts but also doesn't?",
    "I don't know, it's hard to say. Sometimes it's bad, sometimes it's okay.",
    "He says it's fine but he looks terrible to me",
    "First she said it was bad, now she says it's not that bad, I don't know what to think",
    "He's saying one thing but doing another, like he says he's fine but he can barely stand",
    "It's weird, the symptoms keep changing",
    "She was just complaining about one thing, now it's something completely different",
    "I thought it was one thing but now I'm not sure",
    "The pain comes and goes, it's confusing",
    "He rated it a 3 then a 9 within the same minute",
]

INSUFFICIENT_ANSWERS = [
    "I gotta go, just send someone",
    "I can't answer all these questions, just hurry up!",
    "I don't know, I don't know, just come!",
    "Look, I called for help, that's all I know",
    "*click* (caller disconnected)",
    "I... I can't... *inaudible*... just send help",
    "Sorry I'm losing service... can barely hear you...",
    "I don't want to answer any more questions, are you sending someone or not?",
]


def generate_call(call_num, protocol_name, is_ambiguous=False, is_insufficient=False):
    """Generate a single call with 5-15 Q&A rows."""
    call_id = f"CALL-{call_num:03d}"
    timestamp = random_timestamp()
    legacy_names = protocol_to_legacies[protocol_name]
    legacy_used = random.choice(legacy_names)
    name = random_name()
    phone = random_phone()
    addr = random_address()
    dispatch = pick_weighted(DISPATCH_TYPES, DISPATCH_WEIGHTS[protocol_name])
    # Ensure outcome is consistent with dispatch type
    if dispatch == "no_dispatch":
        outcome = "phone_resolved"
    else:
        outcome = pick_weighted(OUTCOMES, OUTCOME_WEIGHTS[protocol_name])
        # If outcome is phone_resolved but we dispatched, pick a different outcome
        while outcome == "phone_resolved" and dispatch != "no_dispatch":
            outcome = pick_weighted(OUTCOMES, OUTCOME_WEIGHTS[protocol_name])

    qa_bank = QA_BANKS[protocol_name]

    if is_insufficient:
        # Short calls: 5-7 questions, many vague answers
        num_questions = random.randint(5, 7)
    else:
        num_questions = random.randint(8, 15)

    # Always start with address and callback, then complaint-specific Qs
    rows = []

    # Q1: Address
    q1_answers = [
        f"It's {addr}",
        f"We're at {addr}",
        f"{addr}, please hurry",
        f"Um, {addr}",
        f"I'm at {addr}, we need help right away",
    ]
    rows.append(("911, what is the address of your emergency?", random.choice(q1_answers)))

    # Q2: Callback number
    q2_answers = [
        phone,
        f"You can reach me at {phone}",
        f"My number is {phone}",
        f"{phone}, that's my cell",
    ]
    rows.append(("What is a callback number?", random.choice(q2_answers)))

    # Select remaining questions from the bank (skip address/phone Qs)
    remaining_needed = num_questions - 2
    # Always include "Tell me what happened" and "How old" first
    complaint_qs = []
    other_qs = []
    for q, answers in qa_bank:
        if q == "Tell me exactly what happened." or q == "How old is the patient?":
            complaint_qs.append((q, answers))
        else:
            other_qs.append((q, answers))

    # Add the opening questions first
    for q, answers in complaint_qs[:2]:
        if answers:
            rows.append((q, random.choice(answers)))
            remaining_needed -= 1

    # Fill remaining from other questions
    if remaining_needed > 0:
        selected = random.sample(other_qs, min(remaining_needed, len(other_qs)))
        for q, answers in selected:
            if answers:
                answer = random.choice(answers)
                # Inject ambiguity
                if is_ambiguous and random.random() < 0.35:
                    answer = random.choice(AMBIGUOUS_ANSWERS)
                # Inject insufficiency
                if is_insufficient and random.random() < 0.4:
                    answer = random.choice(INSUFFICIENT_ANSWERS)
                rows.append((q, answer))

    # If insufficient, maybe truncate further
    if is_insufficient and len(rows) > 6:
        rows = rows[:random.randint(5, 6)]
        # Add a final disconnect/refusal
        rows.append(("Are you still there? I need to ask you a few more questions.",
                      random.choice(INSUFFICIENT_ANSWERS)))

    # Build CSV rows
    call_rows = []
    for i, (question, answer) in enumerate(rows, 1):
        call_rows.append({
            "call_id": call_id,
            "timestamp": timestamp,
            "legacy_protocol_used": legacy_used,
            "complaint_type": protocol_name,
            "question_number": i,
            "dispatcher_question": question,
            "caller_answer": answer,
            "caller_name": name,
            "caller_phone": phone,
            "caller_address": addr,
            "actual_dispatch_type": dispatch,
            "on_scene_outcome": outcome,
        })

    return call_rows


def main():
    all_rows = []
    call_num = 1

    # Build the full list of (protocol, count)
    total_calls = sum(c for _, c in PROTOCOL_COUNTS)
    assert total_calls == 500, f"Expected 500 calls, got {total_calls}"

    # Decide which calls are ambiguous (~15%) or insufficient (~5%)
    ambiguous_count = 75
    insufficient_count = 25
    normal_count = total_calls - ambiguous_count - insufficient_count

    # Assign ambiguous/insufficient flags across all calls proportionally
    call_flags = (["normal"] * normal_count +
                  ["ambiguous"] * ambiguous_count +
                  ["insufficient"] * insufficient_count)
    random.shuffle(call_flags)

    flag_idx = 0
    for protocol_name, count in PROTOCOL_COUNTS:
        for _ in range(count):
            flag = call_flags[flag_idx]
            flag_idx += 1
            is_ambiguous = flag == "ambiguous"
            is_insufficient = flag == "insufficient"
            call_rows = generate_call(
                call_num, protocol_name,
                is_ambiguous=is_ambiguous,
                is_insufficient=is_insufficient,
            )
            all_rows.extend(call_rows)
            call_num += 1

    # Write output
    fieldnames = [
        "call_id", "timestamp", "legacy_protocol_used", "complaint_type",
        "question_number", "dispatcher_question", "caller_answer",
        "caller_name", "caller_phone", "caller_address",
        "actual_dispatch_type", "on_scene_outcome",
    ]

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    # Print stats
    call_ids = set(r["call_id"] for r in all_rows)
    print(f"Generated {len(call_ids)} distinct calls with {len(all_rows)} total rows")
    print(f"Output: {OUTPUT_FILE}")
    print()

    # Distribution
    from collections import Counter
    complaint_counts = Counter()
    for r in all_rows:
        if r["question_number"] == 1:
            complaint_counts[r["complaint_type"]] += 1

    print("Distribution by complaint type:")
    for complaint, count in sorted(complaint_counts.items(), key=lambda x: -x[1]):
        print(f"  {complaint}: {count} calls")

    print()
    rows_per_call = Counter()
    for r in all_rows:
        rows_per_call[r["call_id"]] += 1
    min_rows = min(rows_per_call.values())
    max_rows = max(rows_per_call.values())
    avg_rows = sum(rows_per_call.values()) / len(rows_per_call)
    print(f"Rows per call: min={min_rows}, max={max_rows}, avg={avg_rows:.1f}")


if __name__ == "__main__":
    main()
