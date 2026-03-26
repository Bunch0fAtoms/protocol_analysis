#!/usr/bin/env python3
"""
Generate 10 MPDS-style clinical protocol CSV files for GMR Databricks demo.

Each protocol is a structured clinical triage decision tree used by emergency
medical dispatchers. The data is entirely synthetic.
"""

import csv
import os

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "protocols")

COLUMNS = [
    "protocol_name",
    "row_number",
    "module_name",
    "question_text",
    "condition",
    "disposition",
    "stop_rule",
]


def write_protocol(filename: str, rows: list[dict]):
    """Write a single protocol CSV file."""
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for i, row in enumerate(rows, start=1):
            row["row_number"] = i
            writer.writerow(row)
    print(f"  {filename}: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Helper to build a row dict
# ---------------------------------------------------------------------------
def R(protocol, module, question, condition="", disposition="", stop_rule="CONTINUE"):
    return {
        "protocol_name": protocol,
        "row_number": 0,  # filled by write_protocol
        "module_name": module,
        "question_text": question,
        "condition": condition,
        "disposition": disposition,
        "stop_rule": stop_rule,
    }


# ===================================================================
# 1. CHEST PAIN  (~60 rows)
# ===================================================================
def chest_pain():
    P = "Chest Pain"
    CE = "Case Entry"
    KQ = "Key Questions"
    DA = "Determinant Assignment"
    PAI = "Pre-Arrival Instructions"
    CX = "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing?", "breathing check"),
        R(P, KQ, "Is s/he breathing normally?", "breathing quality"),
        R(P, KQ, "Where exactly is the pain? Can you point to it?", "pain location"),
        R(P, KQ, "Is the pain in the center of the chest?", "substernal pain"),
        R(P, KQ, "Does the pain spread to the arm, jaw, neck, or back?", "pain radiation"),
        R(P, KQ, "How would you describe the pain — is it crushing, squeezing, or pressure-like?", "pain quality"),
        R(P, KQ, "On a scale of 1 to 10, how severe is the pain?", "pain severity"),
        R(P, KQ, "When did the pain start?", "onset time"),
        R(P, KQ, "Has the pain been constant or does it come and go?", "pain pattern"),
        R(P, KQ, "Is s/he sweating more than normal or feeling clammy?", "diaphoresis"),
        R(P, KQ, "Does s/he feel nauseous or has s/he vomited?", "nausea/vomiting"),
        R(P, KQ, "Is s/he short of breath?", "dyspnea"),
        R(P, KQ, "Does s/he feel dizzy or lightheaded?", "dizziness"),
        R(P, KQ, "Has s/he had a heart attack before?", "cardiac history"),
        R(P, KQ, "Does s/he have a history of heart disease or angina?", "cardiac history"),
        R(P, KQ, "Does s/he take nitroglycerin?", "medication history"),
        R(P, KQ, "Has s/he taken any nitroglycerin today? How many?", "medication use"),
        R(P, KQ, "Does s/he take blood thinners or aspirin?", "anticoagulant use"),
        R(P, KQ, "Does s/he have a history of high blood pressure or diabetes?", "comorbidities"),
        R(P, KQ, "Is s/he able to speak in full sentences?", "respiratory effort"),
        R(P, KQ, "What was s/he doing when the pain started?", "activity at onset"),
        R(P, KQ, "Does the pain get worse when s/he breathes in?", "pleuritic component"),
        R(P, KQ, "Is there any swelling in the legs?", "peripheral edema"),
        R(P, KQ, "Has s/he recently had surgery or been immobile for a long time?", "PE risk factors"),
        R(P, KQ, "Does s/he use cocaine or stimulant drugs?", "substance use"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Cardiac arrest — not conscious and not breathing", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "ECHO-level: Agonal breathing with chest pain", "unconscious AND agonal respirations", 0, "STOP"),
        R(P, DA, "DELTA-level: Crushing substernal pain with diaphoresis and radiation to arm/jaw", "substernal pain AND diaphoresis AND radiation", 1, "STOP"),
        R(P, DA, "DELTA-level: Severe difficulty breathing with chest pain", "severe dyspnea AND chest pain", 1, "STOP"),
        R(P, DA, "DELTA-level: Known cardiac history with new onset chest pain and altered consciousness", "cardiac history AND altered LOC", 1, "STOP"),
        R(P, DA, "DELTA-level: Chest pain with syncope or near-syncope", "chest pain AND syncope", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Substernal chest pain with cardiac history, stable vitals", "substernal pain AND cardiac history AND conscious", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Chest pain severity >= 7/10 with dyspnea", "pain >= 7 AND dyspnea", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Chest pain with nitroglycerin unresponsive after 3 doses", "pain persists after 3x NTG", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Moderate chest pain with no radiation or diaphoresis", "moderate pain AND no red flags", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Chest pain improved after nitroglycerin", "pain improved with NTG", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Pleuritic chest pain with recent immobility", "pleuritic pain AND PE risk", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Minor chest pain, non-cardiac features, no risk factors", "mild pain AND no cardiac features AND no history", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Chest wall tenderness reproducible with palpation", "reproducible with palpation", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Chronic stable chest pain, no new symptoms, physician follow-up arranged", "chronic stable AND no change", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Do not let the patient eat or drink anything.", "all dispositions"),
        R(P, PAI, "Have the patient sit up or assume a position of comfort. Do NOT let them lie flat if short of breath.", "conscious patient"),
        R(P, PAI, "If the patient has prescribed nitroglycerin, assist with one dose under the tongue now.", "has NTG AND pain persists"),
        R(P, PAI, "If the patient is not allergic to aspirin and can swallow, have them chew one adult aspirin (325mg) or four baby aspirin.", "no aspirin allergy"),
        R(P, PAI, "Loosen any tight clothing around the chest and neck.", "all dispositions"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR: 30 compressions then 2 breaths. Push hard and fast in the center of the chest.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "If an AED is available, turn it on and follow the voice prompts.", "AED available", "", "CONTINUE"),
        R(P, PAI, "Keep the patient calm and still. Do not let them walk or exert themselves.", "all dispositions"),
        R(P, PAI, "Gather all medications the patient takes and have them ready for the paramedics.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line with me. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If anything changes — if s/he gets worse, becomes unconscious, or stops breathing — call us back immediately.", "all dispositions"),
        R(P, CX, "Do not hang up until I tell you to.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 2. BREATHING PROBLEMS  (~55 rows)
# ===================================================================
def breathing_problems():
    P = "Breathing Problems"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing?", "breathing check"),
        R(P, KQ, "Is s/he breathing normally or is it difficult/labored?", "breathing quality"),
        R(P, KQ, "Can s/he speak in full sentences or only a few words at a time?", "speech assessment"),
        R(P, KQ, "Do you hear any wheezing or whistling sound when s/he breathes?", "wheezing"),
        R(P, KQ, "Do you hear a high-pitched sound when s/he breathes in?", "stridor"),
        R(P, KQ, "Are the lips or fingertips turning blue or gray?", "cyanosis"),
        R(P, KQ, "Is s/he using the muscles in the neck or between the ribs to breathe?", "accessory muscle use"),
        R(P, KQ, "When did the breathing difficulty start?", "onset time"),
        R(P, KQ, "Did it come on suddenly or gradually?", "onset pattern"),
        R(P, KQ, "Was s/he eating or drinking when it started? Could something be stuck in the throat?", "foreign body aspiration"),
        R(P, KQ, "Does s/he have asthma or COPD?", "respiratory history"),
        R(P, KQ, "Does s/he have an inhaler or nebulizer? Has s/he used it?", "inhaler use"),
        R(P, KQ, "How many times has s/he used the inhaler today?", "rescue inhaler frequency"),
        R(P, KQ, "Does s/he have any allergies? Has s/he been exposed to any allergens?", "allergy history"),
        R(P, KQ, "Is there any swelling of the face, tongue, or throat?", "angioedema"),
        R(P, KQ, "Does s/he have a fever or cough?", "infection signs"),
        R(P, KQ, "Is s/he coughing up anything? What color is it?", "sputum assessment"),
        R(P, KQ, "Does s/he have any chest pain?", "associated chest pain"),
        R(P, KQ, "Has s/he had a recent surgery or been on bed rest?", "PE risk factors"),
        R(P, KQ, "Does s/he have a history of heart failure?", "CHF history"),
        R(P, KQ, "Is there swelling in the ankles or legs?", "peripheral edema"),
        R(P, KQ, "Is s/he on home oxygen? What is the flow rate?", "home O2 use"),
        R(P, KQ, "Has s/he had episodes like this before? Was s/he ever intubated?", "prior intubation"),
        R(P, KQ, "Can s/he cough forcefully?", "cough effectiveness"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Not breathing — respiratory arrest", "not breathing AND unconscious", 0, "STOP"),
        R(P, DA, "ECHO-level: Choking — complete obstruction, cannot speak or cough", "complete airway obstruction", 0, "STOP"),
        R(P, DA, "DELTA-level: Severe respiratory distress with cyanosis", "severe distress AND cyanosis", 1, "STOP"),
        R(P, DA, "DELTA-level: Stridor with drooling and unable to swallow", "stridor AND drooling", 1, "STOP"),
        R(P, DA, "DELTA-level: Anaphylaxis — throat swelling with breathing difficulty", "angioedema AND dyspnea", 1, "STOP"),
        R(P, DA, "DELTA-level: Unable to speak, tripod positioning, accessory muscle use", "cannot speak AND tripod AND accessory muscles", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Moderate distress, speaks only a few words at a time", "speaks words only AND wheezing", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Known asthma, inhaler not helping after multiple doses", "asthma AND inhaler ineffective", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Acute dyspnea with history of CHF and lower extremity edema", "CHF AND edema AND dyspnea", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Wheezing with some improvement after inhaler", "wheezing AND partial inhaler response", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Moderate dyspnea with fever and productive cough", "dyspnea AND fever AND cough", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Breathing difficulty with recent surgery or immobility", "dyspnea AND PE risk", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Mild shortness of breath, speaking full sentences, no distress signs", "mild dyspnea AND speaking full sentences", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Known COPD with slight worsening, baseline near-normal", "COPD AND mild exacerbation", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Anxiety-related hyperventilation, no other symptoms", "hyperventilation AND no red flags", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Have the patient sit upright. Do NOT let them lie flat.", "all dispositions"),
        R(P, PAI, "If the patient has a prescribed inhaler, help them use it now — 2 puffs, wait 1 minute, repeat if needed.", "has inhaler"),
        R(P, PAI, "If the patient has an EpiPen and you suspect anaphylaxis, help inject it into the outer thigh.", "anaphylaxis suspected", "", "ELEVATE"),
        R(P, PAI, "Loosen any tight clothing around the chest, neck, and waist.", "all dispositions"),
        R(P, PAI, "If the patient is choking and conscious, perform abdominal thrusts (Heimlich maneuver): stand behind, make a fist above the navel, thrust inward and upward.", "choking AND conscious"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR immediately.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "If on home oxygen, increase the flow rate by 1-2 liters per minute.", "on home O2"),
        R(P, PAI, "Encourage slow, controlled breathing — in through the nose, out through pursed lips.", "conscious AND breathing"),
        # Case Exit
        R(P, CX, "Stay on the line with me. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If the breathing gets worse or s/he stops breathing, call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 3. FALLS / TRAUMATIC INJURY  (~50 rows)
# ===================================================================
def falls_traumatic_injury():
    P = "Falls / Traumatic Injury"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        R(P, CE, "Is the scene safe for you to approach?", "scene safety"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing normally?", "breathing check"),
        R(P, KQ, "How far did s/he fall? What surface did s/he land on?", "mechanism of injury"),
        R(P, KQ, "Did s/he fall from a standing height, or from an elevated surface like a ladder or roof?", "fall height"),
        R(P, KQ, "What part of the body hit first?", "impact point"),
        R(P, KQ, "Did s/he hit the head?", "head injury"),
        R(P, KQ, "Did s/he lose consciousness at any point, even briefly?", "LOC assessment"),
        R(P, KQ, "Is s/he confused or not making sense?", "altered mental status"),
        R(P, KQ, "Is there any bleeding? Where and how much?", "hemorrhage assessment"),
        R(P, KQ, "Is the bleeding controlled or still actively flowing?", "hemorrhage control"),
        R(P, KQ, "Can s/he move all arms and legs?", "motor function"),
        R(P, KQ, "Does s/he have any numbness or tingling in the arms or legs?", "neurological symptoms"),
        R(P, KQ, "Is there pain in the neck or back?", "spinal pain"),
        R(P, KQ, "Is there any obvious deformity to any limb? Does anything look bent wrong?", "deformity assessment"),
        R(P, KQ, "Is there swelling at the injury site?", "swelling"),
        R(P, KQ, "Can s/he bear weight or stand?", "weight bearing"),
        R(P, KQ, "Is s/he on blood thinners like warfarin or Eliquis?", "anticoagulant use"),
        R(P, KQ, "Does s/he have any medical conditions — diabetes, seizures, or a heart condition?", "medical history"),
        R(P, KQ, "Did s/he faint or feel dizzy before the fall?", "pre-fall symptoms"),
        R(P, KQ, "Has s/he vomited since the fall?", "post-injury vomiting"),
        R(P, KQ, "Is there any fluid coming from the ears or nose?", "CSF leak"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Unconscious and not breathing after fall", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Unconscious after fall from significant height", "unconscious AND fall > 6 feet", 1, "STOP"),
        R(P, DA, "DELTA-level: Uncontrolled hemorrhage — blood pooling or spurting", "uncontrolled hemorrhage", 1, "STOP"),
        R(P, DA, "DELTA-level: Suspected spinal injury with neurological deficit", "spinal pain AND numbness/tingling OR motor deficit", 1, "STOP"),
        R(P, DA, "DELTA-level: Open skull fracture or fluid from ears/nose after head impact", "open skull fracture OR CSF leak", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Head injury with brief LOC, now conscious and oriented", "head injury AND brief LOC AND conscious", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Fall on blood thinners with any head impact", "head impact AND anticoagulant use", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Obvious long bone deformity (femur, humerus)", "long bone deformity", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Fall with isolated limb injury, no head or spinal involvement", "limb injury AND no head/spine", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Elderly fall with hip pain, unable to bear weight", "elderly AND hip pain AND non-weight-bearing", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Controlled bleeding with moderate pain", "controlled bleeding AND moderate pain", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Minor fall from standing, small abrasion or bruise, ambulatory", "minor fall AND ambulatory AND minor injury", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Isolated ankle or wrist injury, no deformity, mild swelling", "isolated extremity AND mild swelling AND no deformity", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Fall with no injury, no pain, no head impact, fully ambulatory", "no injury AND no pain AND ambulatory", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Do NOT move the patient if there is any possibility of a neck or back injury.", "suspected spinal injury"),
        R(P, PAI, "If there is serious bleeding, apply firm direct pressure with a clean cloth. Do not remove the cloth — add more on top if it soaks through.", "active hemorrhage"),
        R(P, PAI, "If a limb looks deformed, do not try to straighten it. Keep it still and supported.", "deformity present"),
        R(P, PAI, "If the patient hit their head, do not let them fall asleep. Keep talking to them.", "head injury"),
        R(P, PAI, "Apply ice wrapped in a cloth to any swollen area. 20 minutes on, 20 minutes off.", "swelling present"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "Keep the patient warm with a blanket. Do not give food or drink.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If anything changes — if s/he becomes confused, starts vomiting, or stops breathing — call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 4. ABDOMINAL PAIN  (~45 rows)
# ===================================================================
def abdominal_pain():
    P = "Abdominal Pain"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing normally?", "breathing check"),
        R(P, KQ, "Where exactly is the pain? Can you point to it?", "pain location"),
        R(P, KQ, "When did the pain start?", "onset time"),
        R(P, KQ, "Did the pain come on suddenly or gradually?", "onset pattern"),
        R(P, KQ, "On a scale of 1 to 10, how severe is the pain?", "pain severity"),
        R(P, KQ, "Is the pain constant or does it come in waves?", "pain pattern"),
        R(P, KQ, "Has s/he vomited? How many times? Is there any blood in the vomit?", "vomiting assessment"),
        R(P, KQ, "Is there any blood in the stool or has s/he had black, tarry stools?", "GI bleeding"),
        R(P, KQ, "Does s/he have diarrhea?", "bowel changes"),
        R(P, KQ, "When was the last bowel movement?", "constipation screen"),
        R(P, KQ, "Is the abdomen rigid or hard to the touch?", "peritoneal signs"),
        R(P, KQ, "Does s/he have a fever?", "fever"),
        R(P, KQ, "Is there any chance s/he could be pregnant?", "pregnancy screen"),
        R(P, KQ, "Has s/he had any abdominal surgeries in the past?", "surgical history"),
        R(P, KQ, "Does s/he have a history of kidney stones, gallstones, or ulcers?", "GI/GU history"),
        R(P, KQ, "Is s/he diabetic?", "diabetes"),
        R(P, KQ, "Has s/he eaten anything unusual or potentially contaminated?", "food poisoning screen"),
        R(P, KQ, "Is there any pain in the chest or back associated with the abdominal pain?", "referred pain"),
        R(P, KQ, "Is s/he able to keep fluids down?", "hydration status"),
        R(P, KQ, "Does s/he feel dizzy or lightheaded when standing?", "orthostatic symptoms"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Unconscious with abdominal distension and not breathing", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Rigid abdomen with signs of shock — pale, clammy, rapid pulse", "rigid abdomen AND shock signs", 1, "STOP"),
        R(P, DA, "DELTA-level: Vomiting large amounts of blood or passing large bloody stools", "hematemesis OR massive hematochezia", 1, "STOP"),
        R(P, DA, "DELTA-level: Severe abdominal pain with syncope", "severe pain AND syncope", 1, "STOP"),
        R(P, DA, "DELTA-level: Suspected ruptured ectopic pregnancy — lower abdominal pain, positive pregnancy, lightheaded", "pregnancy AND lower abd pain AND dizziness", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Severe pain (>=8/10) with rigid abdomen, conscious", "severe pain AND rigid abdomen AND conscious", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Abdominal pain with fever >101F and unable to keep fluids down", "pain AND high fever AND vomiting", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Moderate pain with vomiting but no blood, keeping some fluids", "moderate pain AND vomiting AND no bleeding", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Known kidney stone pattern — severe flank pain, writhing", "flank pain AND hx kidney stones", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Mild to moderate abdominal pain, no vomiting, no bleeding, stable", "mild pain AND no red flags", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Mild abdominal discomfort with diarrhea, able to keep fluids down", "mild pain AND diarrhea AND tolerating fluids", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Chronic abdominal discomfort, no new symptoms, physician follow-up planned", "chronic AND stable AND follow-up arranged", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Have the patient lie in a position of comfort — many prefer on their side with knees drawn up.", "conscious patient"),
        R(P, PAI, "Do NOT give the patient anything to eat or drink.", "all dispositions"),
        R(P, PAI, "Do NOT give any pain medication — this may mask symptoms the paramedics need to assess.", "all dispositions"),
        R(P, PAI, "If the patient is vomiting, turn them on their side to prevent choking.", "vomiting"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "Gather all medications and have them ready for the paramedics.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If anything changes — if s/he gets worse or stops breathing — call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 5. ALLERGIC REACTION  (~45 rows)
# ===================================================================
def allergic_reaction():
    P = "Allergic Reaction / Anaphylaxis"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing normally?", "breathing check"),
        R(P, KQ, "What was s/he exposed to? When did the exposure happen?", "allergen identification"),
        R(P, KQ, "Was it a food, medication, insect sting, or something else?", "exposure type"),
        R(P, KQ, "How long ago did the symptoms start?", "symptom onset"),
        R(P, KQ, "Is there any swelling of the face, lips, tongue, or throat?", "angioedema"),
        R(P, KQ, "Is s/he having difficulty breathing or swallowing?", "airway compromise"),
        R(P, KQ, "Do you hear any wheezing or high-pitched breathing sounds?", "bronchospasm"),
        R(P, KQ, "Is there a rash or hives? How widespread?", "urticaria"),
        R(P, KQ, "Is the skin itchy?", "pruritus"),
        R(P, KQ, "Does s/he feel dizzy or lightheaded?", "hypotension symptoms"),
        R(P, KQ, "Has s/he vomited or does s/he have diarrhea?", "GI symptoms"),
        R(P, KQ, "Does s/he have abdominal cramping?", "abdominal symptoms"),
        R(P, KQ, "Has s/he had a severe allergic reaction before?", "allergy history"),
        R(P, KQ, "Does s/he have an EpiPen or epinephrine auto-injector?", "epinephrine availability"),
        R(P, KQ, "Has the EpiPen been used? How many times?", "epinephrine use"),
        R(P, KQ, "Does s/he take any allergy medications like Benadryl?", "antihistamine use"),
        R(P, KQ, "Is the voice hoarse or different than normal?", "laryngeal edema"),
        R(P, KQ, "Does s/he have asthma?", "asthma history"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Anaphylactic shock — unconscious, not breathing", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Throat swelling with severe difficulty breathing", "angioedema AND severe dyspnea", 1, "STOP"),
        R(P, DA, "DELTA-level: Tongue or lip swelling causing airway obstruction", "tongue/lip swelling AND airway compromise", 1, "STOP"),
        R(P, DA, "DELTA-level: Anaphylaxis — hives with breathing difficulty and dizziness/hypotension", "urticaria AND dyspnea AND dizziness", 1, "STOP"),
        R(P, DA, "DELTA-level: Known severe allergy with rapid onset after exposure, altered consciousness", "known allergy AND rapid onset AND altered LOC", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Widespread hives with mild dyspnea, stable after EpiPen", "widespread urticaria AND mild dyspnea AND post-epi", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Facial swelling progressing but airway currently intact", "progressive facial swelling AND airway intact", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Insect sting with systemic reaction — hives beyond sting site", "insect sting AND systemic urticaria", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Moderate hives with no airway involvement, GI symptoms present", "moderate urticaria AND GI symptoms AND no airway", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Allergic reaction with facial swelling, no breathing difficulty", "facial swelling AND no dyspnea", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Localized hives, no systemic symptoms, mild itching", "localized urticaria AND no systemic symptoms", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Insect sting with local swelling only, no spreading", "local reaction only", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Mild rash, known allergy, oral antihistamine taken, symptoms improving", "mild rash AND improving AND antihistamine taken", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "If the patient has an EpiPen: remove the safety cap, press firmly into the outer thigh (through clothing is okay), hold for 10 seconds.", "has EpiPen AND anaphylaxis", "", "CONTINUE"),
        R(P, PAI, "If a second EpiPen is available and symptoms are not improving after 5 minutes, use the second one.", "symptoms persist after first epi"),
        R(P, PAI, "Have the patient lie flat with legs elevated UNLESS they are having difficulty breathing — then sit them upright.", "conscious patient"),
        R(P, PAI, "If the patient has Benadryl (diphenhydramine), give the appropriate dose by mouth if they can swallow safely.", "can swallow AND has antihistamine"),
        R(P, PAI, "Remove the stinger by scraping sideways with a credit card. Do NOT squeeze or use tweezers.", "insect sting"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR immediately.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "Watch for vomiting — turn the patient on their side if they start to vomit.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If the symptoms get worse or s/he stops breathing, call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 6. SEIZURES  (~40 rows)
# ===================================================================
def seizures():
    P = "Seizures"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he seizing right now?", "active seizure"),
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing?", "breathing check"),
        R(P, KQ, "How long has the seizure been going on?", "seizure duration"),
        R(P, KQ, "What does the seizure look like — is the whole body shaking or just one part?", "seizure type"),
        R(P, KQ, "Has the seizure stopped? Is s/he waking up?", "postictal state"),
        R(P, KQ, "Is s/he responsive — can s/he answer questions or follow commands?", "responsiveness"),
        R(P, KQ, "Does s/he have a history of seizures or epilepsy?", "seizure history"),
        R(P, KQ, "Does s/he take seizure medication? Has s/he missed any doses?", "medication compliance"),
        R(P, KQ, "Has s/he had more than one seizure today?", "seizure frequency"),
        R(P, KQ, "Did s/he hit the head during the seizure?", "head injury"),
        R(P, KQ, "Is there any bleeding or injury?", "trauma assessment"),
        R(P, KQ, "Is s/he diabetic? Could the blood sugar be low?", "hypoglycemia screen"),
        R(P, KQ, "Does s/he use drugs or alcohol? Could this be an overdose or withdrawal?", "substance use"),
        R(P, KQ, "Does s/he have a fever? Is s/he hot to the touch?", "febrile seizure screen"),
        R(P, KQ, "Is the patient pregnant?", "eclampsia screen"),
        R(P, KQ, "How old is the patient?", "age"),
        R(P, KQ, "Has s/he been ill recently?", "recent illness"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Seizure with respiratory arrest — not breathing", "seizure AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Status epilepticus — continuous seizure > 5 minutes", "seizure duration > 5 min", 1, "STOP"),
        R(P, DA, "DELTA-level: Multiple seizures without regaining consciousness between them", "serial seizures AND no recovery", 1, "STOP"),
        R(P, DA, "DELTA-level: Seizure in pregnant patient (eclampsia)", "pregnant AND seizure", 1, "STOP"),
        R(P, DA, "DELTA-level: First-time seizure with no known seizure history", "no seizure history AND first event", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Seizure stopped, postictal, known epilepsy, breathing normally", "postictal AND known epilepsy AND breathing", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Seizure with head injury during event", "seizure AND head injury", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Known epilepsy, single seizure, recovering normally, missed medication", "known epilepsy AND single seizure AND recovering AND missed meds", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Febrile seizure in child, seizure has stopped, breathing normally", "febrile AND child AND seizure stopped AND breathing", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Known epilepsy, typical seizure pattern, fully recovered, back to baseline", "known epilepsy AND fully recovered AND at baseline", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Suspected non-epileptic event, fully alert, no injury, physician follow-up", "non-epileptic AND alert AND no injury", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Do NOT put anything in the patient's mouth. Do NOT restrain them.", "active seizure"),
        R(P, PAI, "Clear the area around the patient — move furniture and sharp objects away.", "active seizure"),
        R(P, PAI, "Time the seizure if possible. Note when it started.", "active seizure"),
        R(P, PAI, "When the seizure stops, gently roll the patient onto their side (recovery position).", "seizure stopped"),
        R(P, PAI, "If the patient is diabetic, do NOT give anything by mouth while altered. Wait until fully alert.", "diabetic AND postictal"),
        R(P, PAI, "If the patient stops breathing after the seizure, begin CPR.", "not breathing post-seizure", "", "ELEVATE"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If another seizure starts or s/he stops breathing, call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 7. OVERDOSE / POISONING  (~40 rows)
# ===================================================================
def overdose_poisoning():
    P = "Overdose / Poisoning"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "Is the scene safe? Are there any hazards — needles, chemicals, fumes?", "scene safety"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing?", "breathing check"),
        R(P, KQ, "Is s/he breathing normally — or is it slow, shallow, or irregular?", "breathing quality"),
        R(P, KQ, "What did s/he take or what was s/he exposed to?", "substance identification"),
        R(P, KQ, "How much did s/he take?", "quantity"),
        R(P, KQ, "When did s/he take it? How long ago?", "time of ingestion"),
        R(P, KQ, "Was it intentional or accidental?", "intent"),
        R(P, KQ, "Has s/he vomited?", "vomiting"),
        R(P, KQ, "Are the pupils very small (pinpoint) or very large (dilated)?", "pupil assessment"),
        R(P, KQ, "Is the skin pale, blue, or flushed?", "skin assessment"),
        R(P, KQ, "Is s/he responsive — can s/he answer questions?", "responsiveness"),
        R(P, KQ, "Is s/he confused or agitated?", "mental status"),
        R(P, KQ, "Is s/he having a seizure?", "seizure"),
        R(P, KQ, "Is there any chest pain or irregular heartbeat?", "cardiac symptoms"),
        R(P, KQ, "Does s/he have a history of substance use or prior overdoses?", "substance history"),
        R(P, KQ, "Is there Narcan (naloxone) available?", "naloxone availability"),
        R(P, KQ, "Can you find the pill bottle, container, or packaging? How many are missing?", "container identification"),
        R(P, KQ, "Are there any other people affected?", "mass exposure screen"),
        R(P, KQ, "Does s/he have any other medical conditions?", "medical history"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Unconscious, not breathing — suspected opioid overdose", "unconscious AND not breathing AND opioid suspected", 0, "STOP"),
        R(P, DA, "ECHO-level: Unconscious, not breathing — unknown substance", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Unconscious but breathing, pinpoint pupils — opioid overdose", "unconscious AND breathing AND pinpoint pupils", 1, "STOP"),
        R(P, DA, "DELTA-level: Seizures from overdose", "seizure AND ingestion", 1, "STOP"),
        R(P, DA, "DELTA-level: Intentional ingestion of potentially lethal substance", "intentional AND potentially lethal", 1, "STOP"),
        R(P, DA, "DELTA-level: Chemical exposure with multiple victims", "chemical AND multiple victims", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Conscious but altered, ingested medication, able to give some history", "altered AND conscious AND medication ingestion", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Vomiting blood or having GI symptoms after caustic ingestion", "caustic ingestion AND GI symptoms", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Conscious and alert, accidental double dose of prescription medication", "alert AND accidental AND prescription", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Accidental ingestion in a child, substance identified, child alert", "child AND accidental AND alert", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Accidental minor ingestion, alert, no symptoms, Poison Control consulted", "minor ingestion AND asymptomatic AND Poison Control", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Ingestion of non-toxic substance, no symptoms, Poison Control advises home monitoring", "non-toxic AND asymptomatic", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Do NOT induce vomiting unless instructed by Poison Control.", "all ingestions"),
        R(P, PAI, "If Narcan (naloxone) is available and you suspect opioid overdose: spray one dose into one nostril or inject into the thigh.", "opioid suspected AND naloxone available"),
        R(P, PAI, "If the patient is unconscious but breathing, place in the recovery position on their side.", "unconscious AND breathing"),
        R(P, PAI, "If the patient stops breathing, begin CPR immediately.", "not breathing", "", "ELEVATE"),
        R(P, PAI, "If the patient is conscious, do NOT let them take any more of the substance.", "conscious"),
        R(P, PAI, "Gather the pill bottles, packaging, or containers and have them ready for the paramedics.", "all dispositions"),
        R(P, PAI, "If chemical exposure: move the patient to fresh air if safe to do so. Remove contaminated clothing.", "chemical exposure"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If s/he gets worse, stops breathing, or has a seizure, call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 8. STROKE / NEUROLOGICAL  (~45 rows)
# ===================================================================
def stroke_neurological():
    P = "Stroke / Neurological Emergency"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he conscious?", "consciousness check"),
        R(P, KQ, "Is s/he breathing normally?", "breathing check"),
        R(P, KQ, "Ask the patient to smile. Does one side of the face droop?", "facial droop — FAST"),
        R(P, KQ, "Ask the patient to raise both arms. Does one arm drift downward?", "arm drift — FAST"),
        R(P, KQ, "Ask the patient to repeat a simple sentence like 'The sky is blue.' Is the speech slurred or garbled?", "speech — FAST"),
        R(P, KQ, "What time did the symptoms start? When was s/he last known to be normal?", "time of onset — FAST"),
        R(P, KQ, "Did the symptoms come on suddenly?", "sudden onset"),
        R(P, KQ, "Is there any numbness or weakness on one side of the body?", "unilateral weakness"),
        R(P, KQ, "Is s/he confused or having trouble understanding what you're saying?", "receptive aphasia"),
        R(P, KQ, "Can s/he see normally? Is there any vision loss or double vision?", "visual changes"),
        R(P, KQ, "Does s/he have a sudden severe headache — the worst headache of their life?", "thunderclap headache"),
        R(P, KQ, "Is s/he dizzy or having trouble walking or keeping balance?", "ataxia/vertigo"),
        R(P, KQ, "Has s/he vomited?", "vomiting"),
        R(P, KQ, "Has s/he had a seizure?", "seizure"),
        R(P, KQ, "Does s/he have a history of stroke or TIA?", "stroke history"),
        R(P, KQ, "Does s/he have high blood pressure, diabetes, or atrial fibrillation?", "stroke risk factors"),
        R(P, KQ, "Is s/he on blood thinners?", "anticoagulant use"),
        R(P, KQ, "Has there been any recent head trauma?", "head trauma"),
        R(P, KQ, "Is there any neck pain or stiffness?", "meningismus"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Unconscious and not breathing with neurological symptoms", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Acute stroke signs (FAST positive) within 4.5 hours — time-critical", "FAST positive AND onset < 4.5 hours", 1, "STOP"),
        R(P, DA, "DELTA-level: Sudden worst headache of life with altered consciousness", "thunderclap headache AND altered LOC", 1, "STOP"),
        R(P, DA, "DELTA-level: Acute unilateral weakness/numbness with speech difficulty", "unilateral weakness AND speech changes", 1, "STOP"),
        R(P, DA, "DELTA-level: Stroke symptoms with seizure", "stroke signs AND seizure", 1, "STOP"),
        R(P, DA, "CHARLIE-level: FAST positive, onset > 4.5 hours or unknown, stable", "FAST positive AND onset > 4.5 hours AND stable", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Sudden severe headache with vomiting, no altered consciousness", "severe headache AND vomiting AND alert", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Resolving neurological symptoms — possible TIA", "symptoms resolving AND stroke-like presentation", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Mild facial droop only, no other deficits, onset unclear", "isolated facial droop AND onset unknown", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Known TIA history with recurrent mild symptoms", "TIA history AND mild symptoms", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Mild headache with numbness, fully resolved, no deficits on exam", "resolved symptoms AND normal exam", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Chronic stable neurological complaint, no acute changes, physician follow-up", "chronic stable AND no acute changes", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Note the exact time the symptoms started — this is critical for treatment decisions.", "all stroke dispositions"),
        R(P, PAI, "Do NOT give the patient anything to eat or drink. Stroke can affect swallowing.", "all stroke dispositions"),
        R(P, PAI, "Keep the patient lying flat with the head slightly elevated. Do NOT sit them fully upright.", "conscious patient"),
        R(P, PAI, "Do NOT give aspirin or any medication. The type of stroke must be determined first.", "all stroke dispositions"),
        R(P, PAI, "If the patient vomits, turn them on their side to keep the airway clear.", "vomiting"),
        R(P, PAI, "Keep the patient calm and still. Do not let them walk.", "all dispositions"),
        R(P, PAI, "If the patient becomes unconscious and stops breathing, begin CPR.", "patient becomes unresponsive", "", "ELEVATE"),
        R(P, PAI, "Gather all medications — especially blood thinners — and have them ready for paramedics.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way. Time is critical.", "dispatch confirmed"),
        R(P, CX, "If anything changes — new symptoms, loss of consciousness, or s/he stops breathing — call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 9. PREGNANCY COMPLICATIONS  (~40 rows)
# ===================================================================
def pregnancy_complications():
    P = "Pregnancy Complications"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        # Key Questions
        R(P, KQ, "Is she conscious?", "consciousness check"),
        R(P, KQ, "Is she breathing normally?", "breathing check"),
        R(P, KQ, "How many weeks pregnant is she?", "gestational age"),
        R(P, KQ, "Is she having contractions? How far apart are they?", "contraction assessment"),
        R(P, KQ, "Has her water broken? What color was the fluid?", "rupture of membranes"),
        R(P, KQ, "Is there any vaginal bleeding? How much — spotting, or soaking through pads?", "hemorrhage assessment"),
        R(P, KQ, "Is she in pain? Where is the pain?", "pain assessment"),
        R(P, KQ, "Does she feel the urge to push?", "imminent delivery"),
        R(P, KQ, "Can you see the baby's head or any part of the baby?", "crowning assessment"),
        R(P, KQ, "Has she had a seizure?", "eclampsia screen"),
        R(P, KQ, "Does she have a severe headache or vision changes?", "preeclampsia screen"),
        R(P, KQ, "Is there swelling of the face or hands?", "preeclampsia signs"),
        R(P, KQ, "Is she dizzy or lightheaded?", "hypotension symptoms"),
        R(P, KQ, "Has she had any complications with this pregnancy?", "pregnancy complications"),
        R(P, KQ, "How many previous pregnancies and deliveries has she had?", "obstetric history"),
        R(P, KQ, "Has she had any previous C-sections?", "surgical obstetric history"),
        R(P, KQ, "Is this a high-risk pregnancy?", "risk assessment"),
        R(P, KQ, "Is the baby moving normally?", "fetal movement"),
        R(P, KQ, "Does she have a fever?", "infection screen"),
        R(P, KQ, "Was there any trauma or fall?", "trauma assessment"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Unconscious, not breathing — maternal cardiac arrest", "unconscious AND not breathing", 0, "STOP"),
        R(P, DA, "DELTA-level: Active heavy hemorrhage — soaking through pads continuously", "heavy vaginal bleeding", 1, "STOP"),
        R(P, DA, "DELTA-level: Eclamptic seizure — seizing during pregnancy", "seizure AND pregnant", 1, "STOP"),
        R(P, DA, "DELTA-level: Umbilical cord prolapse — cord visible before baby", "cord prolapse", 1, "STOP"),
        R(P, DA, "DELTA-level: Imminent delivery with complications — breech, cord around neck", "imminent delivery AND complications", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Imminent uncomplicated delivery — baby crowning, contractions < 2 min apart", "crowning AND contractions < 2 min", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Severe preeclampsia — severe headache, visual changes, hypertension symptoms", "severe headache AND visual changes AND edema", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Premature labor (<37 weeks) with active contractions", "preterm AND active contractions", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Moderate vaginal bleeding, stable vital signs, conscious", "moderate bleeding AND stable AND conscious", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Regular contractions < 5 min apart, term pregnancy, no complications", "contractions < 5 min AND term AND no complications", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Mild cramping or spotting, term pregnancy, no other symptoms", "mild symptoms AND term AND stable", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Braxton-Hicks contractions, no bleeding, baby moving normally, follow-up with OB", "Braxton-Hicks AND no bleeding AND fetal movement normal", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "Have her lie on her LEFT side — this improves blood flow to the baby.", "conscious AND pregnant"),
        R(P, PAI, "If delivery is imminent: support the baby's head as it emerges. Do NOT pull on the baby.", "crowning"),
        R(P, PAI, "If the cord is around the baby's neck, gently slip it over the head. Do NOT cut or clamp the cord.", "cord around neck"),
        R(P, PAI, "After delivery, keep the baby at the level of the mother. Dry the baby and keep it warm.", "post-delivery"),
        R(P, PAI, "If heavy bleeding: apply firm pressure with clean towels to the vaginal area. Elevate the legs.", "hemorrhage"),
        R(P, PAI, "Do NOT give the patient anything to eat or drink.", "all dispositions"),
        R(P, PAI, "If she becomes unconscious and stops breathing, begin CPR. If pregnant and > 20 weeks, tilt her slightly to the LEFT during CPR.", "unresponsive AND pregnant", "", "ELEVATE"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way.", "dispatch confirmed"),
        R(P, CX, "If the situation changes — heavier bleeding, baby coming, or she stops breathing — call us back immediately.", "all dispositions"),
    ]
    return rows


# ===================================================================
# 10. UNCONSCIOUS / UNRESPONSIVE  (~50 rows)
# ===================================================================
def unconscious_unresponsive():
    P = "Unconscious / Unresponsive"
    CE, KQ, DA, PAI, CX = "Case Entry", "Key Questions", "Determinant Assignment", "Pre-Arrival Instructions", "Case Exit"
    rows = [
        # Case Entry
        R(P, CE, "Verify the address of the emergency. What is the exact address?", "initial contact"),
        R(P, CE, "What is the callback number you are calling from?", "initial contact"),
        R(P, CE, "Tell me exactly what happened.", "initial contact"),
        R(P, CE, "How old is the patient?", "initial contact"),
        # Key Questions
        R(P, KQ, "Is s/he breathing?", "breathing check"),
        R(P, KQ, "Is s/he breathing normally, or is it gasping, gurgling, or irregular?", "breathing quality"),
        R(P, KQ, "Can you feel a pulse? Check the side of the neck.", "pulse check"),
        R(P, KQ, "Tap the shoulders firmly and shout — does s/he respond at all?", "responsiveness"),
        R(P, KQ, "Does s/he respond to pain? Pinch the earlobe — any reaction?", "pain response"),
        R(P, KQ, "Are the eyes open? Do they respond to your voice?", "eye response"),
        R(P, KQ, "When was s/he last seen normal and awake?", "last known normal"),
        R(P, KQ, "Did anyone witness what happened?", "witness"),
        R(P, KQ, "Did s/he collapse suddenly or gradually become unresponsive?", "onset pattern"),
        R(P, KQ, "Was there any seizure activity — shaking or jerking?", "seizure assessment"),
        R(P, KQ, "Is there any bleeding or sign of trauma?", "trauma assessment"),
        R(P, KQ, "What position is s/he in? Is s/he on the floor, in a chair, in bed?", "position"),
        R(P, KQ, "Are there any pill bottles, syringes, or drugs nearby?", "overdose screen"),
        R(P, KQ, "Is there any vomit? Is the airway clear?", "airway assessment"),
        R(P, KQ, "What color is the skin — normal, pale, blue, or flushed?", "skin color"),
        R(P, KQ, "Is the skin warm, cold, or sweaty?", "skin temperature"),
        R(P, KQ, "Does s/he have any medical conditions — diabetes, epilepsy, heart disease?", "medical history"),
        R(P, KQ, "Does s/he wear a medical alert bracelet or necklace?", "medical ID"),
        R(P, KQ, "Is s/he diabetic? Is there a glucometer available?", "blood sugar check"),
        R(P, KQ, "Has s/he been ill recently?", "recent illness"),
        R(P, KQ, "Could s/he have been exposed to carbon monoxide, fumes, or chemicals?", "toxic exposure"),
        R(P, KQ, "Is anyone else in the building feeling ill?", "mass exposure screen"),
        # Determinant Assignment
        R(P, DA, "ECHO-level: Not breathing and no pulse — cardiac arrest", "no breathing AND no pulse", 0, "STOP"),
        R(P, DA, "ECHO-level: Agonal breathing only (gasping/gurgling) — treat as cardiac arrest", "agonal breathing AND unresponsive", 0, "STOP"),
        R(P, DA, "DELTA-level: Unconscious, not breathing but has a pulse — respiratory arrest", "not breathing AND pulse present", 1, "STOP"),
        R(P, DA, "DELTA-level: Unconscious with signs of major trauma — bleeding, deformity", "unconscious AND major trauma", 1, "STOP"),
        R(P, DA, "DELTA-level: Unconscious with suspected overdose — pinpoint pupils, syringes nearby", "unconscious AND overdose signs", 1, "STOP"),
        R(P, DA, "DELTA-level: Unconscious after sudden collapse — suspected cardiac event", "sudden collapse AND unconscious", 1, "STOP"),
        R(P, DA, "CHARLIE-level: Unconscious but breathing normally, responds to pain", "unconscious AND breathing AND pain response", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Unconscious diabetic — possible hypoglycemia", "unconscious AND diabetic", 2, "CONTINUE"),
        R(P, DA, "CHARLIE-level: Postictal state after witnessed seizure, breathing", "postictal AND breathing", 2, "CONTINUE"),
        R(P, DA, "BRAVO-level: Altered consciousness — responds to voice, confused, not fully alert", "responds to voice AND confused", 3, "CONTINUE"),
        R(P, DA, "BRAVO-level: Fainting episode, now regaining consciousness, oriented", "syncope AND recovering", 3, "CONTINUE"),
        R(P, DA, "ALPHA-level: Brief fainting, now fully alert, oriented, no injury", "brief syncope AND fully alert AND no injury", 4, "CONTINUE"),
        R(P, DA, "ALPHA-level: Near-syncope, did not fully lose consciousness, stable", "near-syncope AND stable", 4, "CONTINUE"),
        R(P, DA, "OMEGA-level: Vasovagal episode with clear trigger, fully recovered, no cardiac history", "vasovagal AND recovered AND no cardiac history", 5, "CONTINUE"),
        # Pre-Arrival Instructions
        R(P, PAI, "If not breathing and no pulse: begin CPR immediately. Place the heel of your hand on the center of the chest. Push hard and fast — at least 2 inches deep, 100-120 pushes per minute.", "cardiac arrest", 0, "STOP"),
        R(P, PAI, "After 30 compressions, tilt the head back, lift the chin, and give 2 breaths. Watch for the chest to rise.", "cardiac arrest CPR"),
        R(P, PAI, "If an AED is available, turn it on and follow the voice prompts. Do not stop CPR until the AED tells you to.", "AED available"),
        R(P, PAI, "If breathing but unconscious: roll the patient onto their side in the recovery position to protect the airway.", "unconscious AND breathing"),
        R(P, PAI, "If there is vomit in the mouth, turn the head to the side and clear it with your finger.", "vomit in airway"),
        R(P, PAI, "If you suspect an opioid overdose and have naloxone (Narcan), administer it now.", "opioid overdose suspected"),
        R(P, PAI, "Do NOT give an unconscious patient anything by mouth.", "unconscious"),
        R(P, PAI, "Keep the patient warm with a blanket.", "all dispositions"),
        # Case Exit
        R(P, CX, "Stay on the line. Help is on the way. Keep doing CPR if instructed — do not stop until paramedics arrive.", "CPR in progress"),
        R(P, CX, "If anything changes, tell me immediately.", "all dispositions"),
        R(P, CX, "Do not hang up until I tell you to.", "all dispositions"),
    ]
    return rows


# ===================================================================
# Main
# ===================================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Generating MPDS-style clinical protocol CSV files...\n")

    protocols = [
        ("chest_pain.csv", chest_pain()),
        ("breathing_problems.csv", breathing_problems()),
        ("falls_traumatic_injury.csv", falls_traumatic_injury()),
        ("abdominal_pain.csv", abdominal_pain()),
        ("allergic_reaction.csv", allergic_reaction()),
        ("seizures.csv", seizures()),
        ("overdose_poisoning.csv", overdose_poisoning()),
        ("stroke_neurological.csv", stroke_neurological()),
        ("pregnancy_complications.csv", pregnancy_complications()),
        ("unconscious_unresponsive.csv", unconscious_unresponsive()),
    ]

    total = 0
    for filename, rows in protocols:
        write_protocol(filename, rows)
        total += len(rows)

    print(f"\nTotal: {total} rows across {len(protocols)} protocol files")
    print(f"Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
