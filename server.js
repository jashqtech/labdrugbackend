const express = require('express');
const fs = require('fs');
const fs1 = require('fs').promises;
const path = require('path');
const { parse } = require('csv-parse/sync');
const { stringify } = require('csv-stringify/sync');
const axios = require('axios');
require('dotenv').config();
const multer = require('multer');
const cors = require('cors');
const PDFJS = require('pdfjs-dist/legacy/build/pdf.js');
const pdfjsWorker = require('pdfjs-dist/legacy/build/pdf.worker.js');
PDFJS.GlobalWorkerOptions.workerSrc = pdfjsWorker;
const Tesseract = require('tesseract.js');
const AWS = require('aws-sdk');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const altNames = require('./altNames');
const fullBiomarkerList = require('./fullBiomarkerList');

const app = express();
const PORT = 3000;

app.use(express.json());
app.use(cors());

// Multer for single upload (code1 style)
const uploadSingle = multer({ dest: 'uploads/' });

// Multer for multi upload (code2 style)
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'uploads/'),
  filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`)
});
const uploadMulti = multer({ 
  storage,
  limits: { fileSize: 50 * 1024 * 1024 }
});

let rules = [];
const CSV_HEADERS = [
  'Based_On',
  'Drug Category',
  'Drug Class',
  'Medications',
  'AHA Lab Trigger - Organs Problematic',
  'AHA Lab Trigger - Organs Dysfunctional',
  'AHA Lab Trigger - Biomarker Abnormal',
  'AHA Lab Trigger - Biomarker Low',
  'AHA Lab Trigger - Biomarker High',
  'Caution Note',
  'ICD-10 Diagnosis',
  'ICD-10 Diagnostic Code',
  'SNOMED',
  '', // Empty column 1
  '' // Empty column 2
];

let abnormalRules = [];

function loadAbnormalRules() {
  const csvPath = path.join(__dirname, 'abnormalrules.csv');
  console.log(`[INIT] Loading abnormal CSV from ${csvPath}`);

  try {
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const rows = parse(csv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    });

    abnormalRules = rows.map(r => ({
      order: r.Order,
      panel: r.Panel,
      field: r.Field,
      biomarkerName: r.Biomarker_Name.trim(),
      units: r['Conventional Units'],
      manLower: r['Man Abnormal Lower Limit'] ? parseFloat(r['Man Abnormal Lower Limit'].replace(/[^0-9.]/g, '')) : null,
      manUpper: r['Man Abnormal Upper Limit'] ? parseFloat(r['Man Abnormal Upper Limit'].replace(/[^0-9.]/g, '')) : null,
      womanLower: r['Woman Abnormal Lower Limit'] ? parseFloat(r['Woman Abnormal Lower Limit'].replace(/[^0-9.]/g, '')) : null,
      womanUpper: r['Woman Abnormal Upper Limit'] ? parseFloat(r['Woman Abnormal Upper Limit'].replace(/[^0-9.]/g, '')) : null,
    }));

    console.log(`[INIT] Loaded ${abnormalRules.length} abnormal rules`);
  } catch (e) {
    console.error('[ERROR] Abnormal CSV load error:', e);
  }
}

function getAbnormalBiomarkers(biomarkers, patientSex) {
  const sex = patientSex.toLowerCase() === 'male' ? 'man' : 'woman';
  const abnormals = [];

  for (const [key, value] of Object.entries(biomarkers)) {
    const rule = abnormalRules.find(r =>
      r.biomarkerName.toLowerCase() === key.toLowerCase() ||
      r.field.toLowerCase() === key.toLowerCase()
    );
    if (!rule) continue;

    const lower = rule[`${sex}Lower`];
    const upper = rule[`${sex}Upper`];

    let status = '';
    if (lower !== null && value < lower) {
      status = 'low';
    } else if (upper !== null && value > upper) {
      status = 'high';
    }

    if (status) {
      abnormals.push(`abnormal ${status} ${rule.biomarkerName}`);
    }
  }
  console.log('abnormal biomarkers --- ', abnormals)
  return abnormals;
}

function loadRules() {
  const csvPath = path.join(__dirname, 'rules1.csv');
  console.log(`[INIT] Loading CSV from ${csvPath}`);

  try {
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const rows = parse(csv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true,
    });

    console.log(`[INIT] Parsed ${rows.length} rule rows from CSV`);

    rules = [];
    rows.forEach((r, index) => {
      const basedOn = r['Based_On']?.trim() || '';
      const drugCategory = r['Drug Category']?.trim() || '';
      const drugClass = r['Drug Class']?.trim() || '';
      const medsRaw = r['Medications']?.trim() || '';
      const cautionNote = r['Caution Note']?.trim() || '';
      const cautionHeadline = r['Caution Headline']?.trim() || '';
      const icd10 = r['ICD-10 Diagnostic Code']?.trim() || '';
      const snomed = r['SNOMED']?.trim() || '';
      const ahaLabTriggerOrgansProblematic = r['AHA Lab Trigger - Organs Problematic']?.trim() || '';
      const ahaLabTriggerOrgansDysfunctional = r['AHA Lab Trigger - Organs Dysfunctional']?.trim() || '';
      const ahaLabTriggerBiomarkerAbnormal = r['AHA Lab Trigger - Biomarker Abnormal']?.trim() || '';
      const ahaLabTriggerBiomarkerLow = r['AHA Lab Trigger - Biomarker Low']?.trim() || '';
      const ahaLabTriggerBiomarkerHigh = r['AHA Lab Trigger - Biomarker High']?.trim() || '';

      if (!drugClass || !medsRaw) return;

      const medsSet = new Set(
        medsRaw
          .split(',')
          .map(m => m.trim().toLowerCase())
          .filter(Boolean)
      );

      rules.push({
        rowIndex: index + 2,
        basedOn,
        drugCategory,
        drugClass,
        medications: medsSet,
        medicationsArray: Array.from(medsSet),
        cautionHeadline,
        cautionNote,
        icd10,
        snomed,
        ahaLabTriggerOrgansProblematic,
        ahaLabTriggerOrgansDysfunctional,
        ahaLabTriggerBiomarkerAbnormal,
        ahaLabTriggerBiomarkerLow,
        ahaLabTriggerBiomarkerHigh,
      });
    });

    console.log(`[INIT] Loaded ${rules.length} valid rules for lookup`);
  } catch (e) {
    console.error('[ERROR] CSV load error:', e);
  }
}

loadRules();
loadAbnormalRules();

function processOrganData(organData) {
  console.log('\n[ORGAN PROCESSING] Starting organ data analysis...');
  console.log(`[ORGAN PROCESSING] OrganData type:`, typeof organData);

  if (!organData || Object.keys(organData).length === 0) {
    console.log('[ORGAN PROCESSING] No organ data available');
    return [];
  }

  try {
    const parsedOrganData = typeof organData === 'string' ? JSON.parse(organData) : organData;
    console.log('[ORGAN PROCESSING] Parsed organ data:', JSON.stringify(parsedOrganData, null, 2));

    const affectedOrgans = [];

    for (const [organName, organInfo] of Object.entries(parsedOrganData)) {
      const finalScore = organInfo.finalScore;
      console.log(`[ORGAN PROCESSING] Analyzing ${organName}: finalScore = ${finalScore}`);

      let status = null;

      if (finalScore === 6) {
        status = 'stressed';
        console.log(`[ORGAN PROCESSING] ✓ ${organName} is STRESSED (score: 6)`);
      } else if (finalScore >= 7 && finalScore <= 8) {
        status = 'problematic';
        console.log(`[ORGAN PROCESSING] ✓ ${organName} is PROBLEMATIC (score: 7-8)`);
      } else if (finalScore >= 9 && finalScore <= 11) {
        status = 'dysfunctional';
        console.log(`[ORGAN PROCESSING] ✓ ${organName} is DYSFUNCTIONAL (score: 9-11)`);
      } else {
        console.log(`[ORGAN PROCESSING] ○ ${organName} is NORMAL (score: ${finalScore})`);
      }

      if (status) {
        affectedOrgans.push({
          status,
          organName: organName.toLowerCase(),
          finalScore,
          display: `${status} ${organName.toLowerCase()}`
        });
      }
    }

    // Sort globally by score desc for logging (optional, but consistent)
    affectedOrgans.sort((a, b) => b.finalScore - a.finalScore);
    const resultString = affectedOrgans.map(item => item.display).join(', ');
    console.log(`[ORGAN PROCESSING] Final affected organs (sorted by score desc): "${resultString}"`);
    return affectedOrgans;

  } catch (error) {
    console.error('[ORGAN PROCESSING] ERROR parsing organ data:', error.message);
    return [];
  }
}

async function callLabResultsAPI(organizationId, patientId, biomarkers, labDate, bearerToken) {
  console.log('\n[LAB API] Calling lab results API...');
  console.log(`[LAB API] Organization ID: ${organizationId}`);
  console.log(`[LAB API] Patient ID: ${patientId}`);
  console.log(`[LAB API] Lab Date: ${labDate}`);
  console.log(`[LAB API] Biomarkers:`, JSON.stringify(biomarkers, null, 2));

  try {
    const dateParts = labDate.split('/');
    const isoDate = new Date(`${dateParts[2]}-${dateParts[0]}-${dateParts[1]}`).toISOString();
    console.log(`[LAB API] Converted date to ISO: ${isoDate}`);

    const url = `https://21rn85vlfa.execute-api.us-east-1.amazonaws.com/organizations/${organizationId}/patients/${patientId}/lab-results`;
    console.log(`[LAB API] URL: ${url}`);

    const payload = {
      biomarkers: biomarkers,
      diagnosticResultDate: isoDate
    };

    console.log(`[LAB API] Request payload:`, JSON.stringify(payload, null, 2));

    const response = await axios.post(url, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${bearerToken}`
      },
      timeout: 30000
    });

    console.log('[LAB API] ✓ Successfully received response');

    return response.data;

  } catch (error) {
    console.error('[LAB API] ERROR:', error.message);
    if (error.response) {
      console.error('[LAB API] Error response status:', error.response.status);
      console.error('[LAB API] Error response data:', error.response.data);
    }
    throw error;
  }
}

function getAllDrugClasses() {
  const classRules = rules.filter(r => r.basedOn.toLowerCase() === 'class');
  return [...new Set(classRules.map(r => r.drugClass))];
}

function exactMedicationMatch(medName) {
  console.log(`\n[STEP 1] Searching for exact match: "${medName}"`);
  const lowerMedName = medName.toLowerCase();

  for (const rule of rules) {
    if (rule.medications.has(lowerMedName)) {
      console.log(`[STEP 1] ✓ EXACT MATCH FOUND in row ${rule.rowIndex}`);
      console.log(`[STEP 1] Drug Class: "${rule.drugClass}", Based On: "${rule.basedOn}"`);
      return rule;
    }
  }

  console.log(`[STEP 1] ✗ No exact match found for "${medName}"`);
  return null;
}

async function identifyDrugClassWithGemini(medName) {
  console.log(`\n[STEP 2] Querying Gemini API for medication: "${medName}"`);

  const drugClasses = getAllDrugClasses();
  console.log(`[STEP 2] Available drug classes in CSV: ${drugClasses.length} classes`);

  const drugClassMap = {};
  rules.filter(r => r.basedOn.toLowerCase() === 'class').forEach(rule => {
    if (!drugClassMap[rule.drugClass]) {
      drugClassMap[rule.drugClass] = [];
    }
    drugClassMap[rule.drugClass].push(...rule.medicationsArray);
  });

  const prompt = `You are a pharmaceutical expert. I need to identify which drug class a medication belongs to.

Medication Name: "${medName}"

Available Drug Classes in our database:
${Object.entries(drugClassMap).map(([className, meds]) =>
    `- ${className}`
  ).join('\n')}

Please analyze and respond in the following JSON format ONLY (no additional text):
{
  "foundInDatabase": true/false,
  "drugClass": "exact drug class name from the list above if found, or null",
  "actualDrugClass": "the real-world drug class this medication belongs to",
  "confidence": "high/medium/low",
  "explanation": "brief explanation"
}

If the medication belongs to one of the listed drug classes, set foundInDatabase to true and provide the exact drug class name.
If not, set foundInDatabase to false and provide the actual real-world drug class it belongs to.`;

  try {
    const geminiApiKey = process.env.GEMINI_API_KEY;

    if (!geminiApiKey) {
      console.error('[STEP 2] ERROR: GEMINI_API_KEY is not defined in environment variables');
      throw new Error("GEMINI_API_KEY is not defined");
    }

    console.log('[STEP 2] Sending request to Gemini API...');

    const geminiResponse = await axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${geminiApiKey}`,
      {
        contents: [{ parts: [{ text: prompt }] }]
      },
      {
        headers: { 'Content-Type': 'application/json' },
        timeout: 50000
      }
    );

    console.log('[STEP 2] ✓ Received response from Gemini API');

    const responseText = geminiResponse.data?.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!responseText) {
      console.error('[STEP 2] ERROR: No valid response from Gemini');
      return null;
    }

    console.log('[STEP 2] Raw Gemini response:', responseText);

    let jsonText = responseText.trim();
    if (jsonText.startsWith('```json')) {
      jsonText = jsonText.replace(/```json\n?/g, '').replace(/```\n?/g, '');
    } else if (jsonText.startsWith('```')) {
      jsonText = jsonText.replace(/```\n?/g, '');
    }

    const parsedResponse = JSON.parse(jsonText);
    console.log('[STEP 2] Parsed Gemini analysis:', JSON.stringify(parsedResponse, null, 2));

    return parsedResponse;

  } catch (apiError) {
    console.error('[STEP 2] ERROR: Gemini API call failed:', apiError.message);
    if (apiError.response) {
      console.error('[STEP 2] API Error Response:', apiError.response.data);
    }
    return null;
  }
}

function matchDrugClassToRule(drugClassName, medName) {
  console.log(`\n[STEP 3] Matching drug class "${drugClassName}" to rules`);

  const matchingRules = rules.filter(r =>
    r.drugClass.toLowerCase() === drugClassName.toLowerCase()
  );

  console.log(`[STEP 3] Found ${matchingRules.length} matching rules`);

  if (matchingRules.length === 0) {
    console.log(`[STEP 3] ✗ No matching rules found for drug class "${drugClassName}"`);
    return null;
  }

  const classBasedRule = matchingRules.find(r => r.basedOn.toLowerCase() === 'class');

  if (classBasedRule) {
    console.log(`[STEP 3] ✓ Found CLASS-based rule at row ${classBasedRule.rowIndex}`);
    return classBasedRule;
  }

  console.log(`[STEP 3] ⚠ No CLASS-based rule found, using first match at row ${matchingRules[0].rowIndex}`);
  return matchingRules[0];
}

function addMedicationToCSV(drugClass, medName) {
  console.log(`\n[UPDATE CSV] Adding "${medName}" to drug class "${drugClass}" in CSV`);

  try {
    const csvPath = path.join(__dirname, 'rules1.csv');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');

    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: false,
      relax_column_count: true,
      trim: false,
    });

    let updated = false;

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const basedOn = record['Based_On']?.trim() || '';
      const rowDrugClass = record['Drug Class']?.trim() || '';

      if (rowDrugClass === drugClass && basedOn.toLowerCase() === 'class') {
        const currentMeds = record['Medications']?.trim() || '';
        const medArray = currentMeds
          .split(',')
          .map(m => m.trim())
          .filter(Boolean);

        if (!medArray.some(m => m.toLowerCase() === medName.toLowerCase())) {
          medArray.push(medName);
          record['Medications'] = medArray.join(', ');

          updated = true;
          console.log(`[UPDATE CSV] ✓ Added "${medName}" to row ${i + 2}`);
          console.log(`[UPDATE CSV] Updated medications: ${record['Medications']}`);
          break;
        } else {
          console.log(`[UPDATE CSV] ⚠ "${medName}" already exists in row ${i + 2}`);
          return;
        }
      }
    }

    if (updated) {
      const output = stringify(records, {
        header: true,
        columns: CSV_HEADERS,
      });

      fs.writeFileSync(csvPath, output);
      console.log('[UPDATE CSV] ✓ CSV file updated successfully');

      loadRules();
      console.log('[UPDATE CSV] ✓ Rules reloaded from updated CSV');
    } else {
      console.log(`[UPDATE CSV] ✗ Could not find CLASS-based rule for "${drugClass}"`);
    }

  } catch (error) {
    console.error('[UPDATE CSV] ERROR:', error.message);
    console.error(error.stack);
  }
}

function addNewDrugClassToCSV(drugClass, medName, geminiResult) {
  console.log(`\n[ADD NEW CLASS] Adding new drug class "${drugClass}" with medication "${medName}"`);

  try {
    const csvPath = path.join(__dirname, 'rules1.csv');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');

    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: false,
      relax_column_count: true,
      trim: false,
    });

    const newRecord = {
      'Based_On': 'Class',
      'Drug Category': '',
      'Drug Class': drugClass,
      'Medications': medName,
      'AHA Lab Trigger - Organs Problematic': '',
      'AHA Lab Trigger - Organs Dysfunctional': '',
      'AHA Lab Trigger - Biomarker Abnormal': '',
      'AHA Lab Trigger - Biomarker Low': '',
      'AHA Lab Trigger - Biomarker High': '',
      'Caution Note': '',
      'ICD-10 Diagnosis': 'Side effect of drug',
      'ICD-10 Diagnostic Code': 'T88.7XXA',
      'SNOMED': '69449002',
      '': '',
      ' ': ''
    };

    records.push(newRecord);

    const output = stringify(records, {
      header: true,
      columns: CSV_HEADERS,
    });

    fs.writeFileSync(csvPath, output);
    console.log('[ADD NEW CLASS] ✓ New drug class added to CSV');

    loadRules();
    console.log('[ADD NEW CLASS] ✓ Rules reloaded from updated CSV');

  } catch (error) {
    console.error('[ADD NEW CLASS] ERROR:', error.message);
    console.error(error.stack);
  }
}

function filterAbnormalBiomarkers(abnormalDescriptions, rule) {
  if (!abnormalDescriptions || abnormalDescriptions.length === 0 || !rule) {
    return [];
  }

  console.log(`[FILTER BIOMARKERS] Filtering ${abnormalDescriptions.length} abnormals for rule row ${rule.rowIndex} (Drug Class: ${rule.drugClass})`);

  // Parse triggers from rule
  const abnormalTriggers = rule.ahaLabTriggerBiomarkerAbnormal
    ? rule.ahaLabTriggerBiomarkerAbnormal.split(',').map(t => t.trim().toLowerCase()).filter(Boolean)
    : [];
  const lowTriggers = rule.ahaLabTriggerBiomarkerLow
    ? rule.ahaLabTriggerBiomarkerLow.split(',').map(t => t.trim().toLowerCase()).filter(Boolean)
    : [];
  const highTriggers = rule.ahaLabTriggerBiomarkerHigh
    ? rule.ahaLabTriggerBiomarkerHigh.split(',').map(t => t.trim().toLowerCase()).filter(Boolean)
    : [];

  console.log(`[FILTER BIOMARKERS] Abnormal triggers: [${abnormalTriggers.join(', ')}]`);
  console.log(`[FILTER BIOMARKERS] Low triggers: [${lowTriggers.join(', ')}]`);
  console.log(`[FILTER BIOMARKERS] High triggers: [${highTriggers.join(', ')}]`);

  const filtered = [];
  const included = new Set(); // To avoid duplicates

  for (const desc of abnormalDescriptions) {
    const parts = desc.split(' ');
    if (parts.length < 3) continue; // Invalid format

    const direction = parts[1]; // 'low' or 'high'
    const biomarkerName = parts.slice(2).join(' ').toLowerCase();
    const fullDesc = desc; // Keep original casing for specific matches
    const genericDesc = `abnormal ${parts.slice(2).join(' ')}`; // Generic for abnormal trigger match

    if (included.has(genericDesc)) continue; // Avoid duplicates on generic

    let shouldInclude = false;
    let toInclude = fullDesc; // Default to full if specific match

    // Check low triggers first
    if (direction === 'low' && lowTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
      toInclude = fullDesc;
    }
    // Check high triggers
    else if (direction === 'high' && highTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
      toInclude = fullDesc;
    }
    // Check abnormal triggers (any direction) - use generic
    else if (abnormalTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
      toInclude = genericDesc;
    }

    if (shouldInclude) {
      filtered.push(toInclude);
      included.add(genericDesc);
      console.log(`[FILTER BIOMARKERS] ✓ Included: ${toInclude} (direction: ${direction}, biomarker: ${biomarkerName})`);
    } else {
      console.log(`[FILTER BIOMARKERS] ○ Excluded: ${fullDesc}`);
    }
  }

  console.log(`[FILTER BIOMARKERS] Filtered down to ${filtered.length} matching abnormals`);
  return filtered;
}

function filterAffectedOrgans(affectedOrgansArray, rule) {
  if (!affectedOrgansArray || affectedOrgansArray.length === 0 || !rule) {
    console.log(`[FILTER ORGANS] No affected organs array or rule provided`);
    return '';
  }

  console.log(`[FILTER ORGANS] Filtering ${affectedOrgansArray.length} affected organs for rule row ${rule.rowIndex} (Drug Class: ${rule.drugClass})`);

  // Parse problematic and dysfunctional triggers from rule
  const problematicTriggers = rule.ahaLabTriggerOrgansProblematic
    ? rule.ahaLabTriggerOrgansProblematic.split(',').map(t => t.trim().toLowerCase()).filter(Boolean)
    : [];
  const dysfunctionalTriggers = rule.ahaLabTriggerOrgansDysfunctional
    ? rule.ahaLabTriggerOrgansDysfunctional.split(',').map(t => t.trim().toLowerCase()).filter(Boolean)
    : [];

  console.log(`[FILTER ORGANS] Problematic triggers: [${problematicTriggers.join(', ')}]`);
  console.log(`[FILTER ORGANS] Dysfunctional triggers: [${dysfunctionalTriggers.join(', ')}]`);

  const filtered = [];

  for (const item of affectedOrgansArray) {
    const { status, organName, finalScore, display } = item;
    let shouldInclude = false;

    if (status === 'problematic') {
      shouldInclude = problematicTriggers.some(trigger => 
        organName.includes(trigger) || trigger.includes(organName)
      );
    } else if (status === 'dysfunctional') {
      shouldInclude = dysfunctionalTriggers.some(trigger => 
        organName.includes(trigger) || trigger.includes(organName)
      );
    }
    // Stressed organs are not checked (excluded unless you add a trigger column)

    if (shouldInclude) {
      filtered.push(item);
      console.log(`[FILTER ORGANS] ✓ Included: ${display} (status: ${status}, organ: ${organName}, score: ${finalScore})`);
    } else {
      console.log(`[FILTER ORGANS] ○ Excluded: ${display} (status: ${status}, organ: ${organName}, score: ${finalScore})`);
    }
  }

  // Sort filtered by finalScore descending (dysfunctional > problematic > stressed)
  filtered.sort((a, b) => b.finalScore - a.finalScore);

  const result = filtered.map(item => item.display).join(', ');
  console.log(`[FILTER ORGANS] Filtered and sorted down to: "${result}"`);
  return result;
}

async function processMedication(medName, dose, date, affectedOrgansArray = [], abnormalBiomarkers = []) {
  const affectedOrgansString = affectedOrgansArray.map(item => item.display).join(', ');
  console.log('\n' + '='.repeat(80));
  console.log(`[PROCESSING] Medication: "${medName}"`);
  console.log(`[PROCESSING] Affected Organs from Lab: "${affectedOrgansString}"`);
  console.log(`[PROCESSING] Total Abnormal Biomarkers: ${abnormalBiomarkers.length}`);
  console.log('='.repeat(80));

  if (!medName || typeof medName !== 'string') {
    console.log('[PROCESSING] ✗ Invalid medication name');
    return createBlankResponse(medName, dose, date, 'Invalid name', null, [], '');
  }

  let matchedRule = exactMedicationMatch(medName);

  if (matchedRule) {
    console.log(`[PROCESSING] ✓ STEP 1 succeeded - exact match found`);
    const filteredAbnormals = filterAbnormalBiomarkers(abnormalBiomarkers, matchedRule);
    const filteredOrgans = filterAffectedOrgans(affectedOrgansArray, matchedRule);
    return formatResponse(medName, dose, date, matchedRule, 'exact_match', null, filteredOrgans, filteredAbnormals);
  }

  console.log(`[PROCESSING] Proceeding to STEP 2 - Gemini API lookup`);
  const geminiResult = await identifyDrugClassWithGemini(medName);

  if (!geminiResult) {
    console.log('[PROCESSING] ✗ STEP 2 failed - Gemini API error');
    return createBlankResponse(medName, dose, date, 'Gemini API error', null, [], '');
  }

  let drugClassToMatch = null;

  if (geminiResult.foundInDatabase && geminiResult.drugClass) {
    console.log(`[PROCESSING] ✓ STEP 2 succeeded - found in database: "${geminiResult.drugClass}"`);
    drugClassToMatch = geminiResult.drugClass;
  } else if (geminiResult.actualDrugClass) {
    console.log(`[PROCESSING] ⚠ STEP 2 - medication belongs to NEW drug class: "${geminiResult.actualDrugClass}"`);
    console.log(`[PROCESSING] ⚠ Adding to CSV but SKIPPING response for this request`);

    addNewDrugClassToCSV(geminiResult.actualDrugClass, medName, geminiResult);

    return {
      _skipInResponse: true,
      reason: 'new_class_added',
      medName,
      drugClass: geminiResult.actualDrugClass,
      message: `New drug class "${geminiResult.actualDrugClass}" added to database. Medication will be available in next request.`
    };
  } else {
    console.log('[PROCESSING] ✗ STEP 2 failed - could not identify drug class');
    return createBlankResponse(medName, dose, date, 'Drug class not identified', null, [], '');
  }

  console.log(`[PROCESSING] Proceeding to STEP 3 - matching drug class to rules`);
  matchedRule = matchDrugClassToRule(drugClassToMatch, medName);

  if (!matchedRule) {
    console.log('[PROCESSING] ✗ STEP 3 failed - no matching rule found');
    return createBlankResponse(medName, dose, date, `Drug class "${drugClassToMatch}" not found in rules`, geminiResult, [], '');
  }

  console.log(`[PROCESSING] ✓ STEP 3 succeeded - matched to row ${matchedRule.rowIndex}`);

  addMedicationToCSV(matchedRule.drugClass, medName);

  const filteredAbnormals = filterAbnormalBiomarkers(abnormalBiomarkers, matchedRule);
  const filteredOrgans = filterAffectedOrgans(affectedOrgansArray, matchedRule);
  return formatResponse(medName, dose, date, matchedRule, 'gemini_match', geminiResult, filteredOrgans, filteredAbnormals);
}

// Core processing function extracted from /test endpoint
async function processTestRequest(body, bearerToken) {
  console.log('\n\n' + '█'.repeat(80));
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█' + '  NEW REQUEST RECEIVED'.padEnd(78) + '█');
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█'.repeat(80) + '\n');
  console.log('[REQUEST] Body:', JSON.stringify(body, null, 2));

  const {
    labDate,
    patientSex,
    biomarkers,
    medicationList,
    organizationId,
    patientId
  } = body;

  if (!labDate || !patientSex || !biomarkers || !medicationList || !organizationId || !patientId) {
    console.log('[REQUEST] ✗ Missing required fields');
    throw new Error('Required fields: labDate, patientSex, biomarkers, medicationList, organizationId, patientId');
  }

  if (!Array.isArray(medicationList) || !medicationList.length) {
    console.log('[REQUEST] ✗ Invalid request - medicationList must be a non-empty array');
    throw new Error('medicationList must be a non-empty array');
  }

  console.log(`[REQUEST] Lab Date: ${labDate}`);
  console.log(`[REQUEST] Patient Sex: ${patientSex}`);
  console.log(`[REQUEST] Organization ID: ${organizationId}`);
  console.log(`[REQUEST] Patient ID: ${patientId}`);
  console.log(`[REQUEST] Processing ${medicationList.length} medication(s)`);

  console.log(`[REQUEST] Bearer token extracted: ${bearerToken.substring(0, 20)}...`);

  // STEP 1: Call Lab Results API
  console.log('\n' + '▓'.repeat(80));
  console.log('▓  STEP 1: Calling Lab Results API'.padEnd(79) + '▓');
  console.log('▓'.repeat(80));

  const labResultsResponse = await callLabResultsAPI(
    organizationId,
    patientId,
    biomarkers,
    labDate,
    bearerToken
  );

  // STEP 2: Process OrganData
  console.log('\n' + '▓'.repeat(80));
  console.log('▓  STEP 2: Processing Organ Data'.padEnd(79) + '▓');
  console.log('▓'.repeat(80));

  const organDataString = labResultsResponse.OrganData || '{}';
  const affectedOrgansArray = processOrganData(organDataString);
  const affectedOrgansString = affectedOrgansArray.map(item => item.display).join(', ');
  console.log(`[REQUEST] Affected organs to use for medications: "${affectedOrgansString}"`);

  // STEP 2.5: Compute abnormal biomarkers
  console.log('\n' + '▓'.repeat(80));
  console.log('▓  STEP 2.5: Computing Abnormal Biomarkers'.padEnd(79) + '▓');
  console.log('▓'.repeat(80));
  const filteredBiomarkers = Object.fromEntries(
    Object.entries(biomarkers).filter(([_, value]) =>
      value !== null && value !== undefined && value !== ''
    )
  );

  const abnormalDescriptions = getAbnormalBiomarkers(filteredBiomarkers, patientSex);
  console.log(`[REQUEST] Abnormal biomarkers: [${abnormalDescriptions.join(', ')}]`);

  // STEP 3: Process Medications
  console.log('\n' + '▓'.repeat(80));
  console.log('▓  STEP 3: Processing Medications'.padEnd(79) + '▓');
  console.log('▓'.repeat(80));

  const results = [];
  const skippedMedications = [];

  for (let i = 0; i < medicationList.length; i++) {
    const med = medicationList[i];
    console.log(`\n[REQUEST] Processing medication ${i + 1}/${medicationList.length}`);

    const result = await processMedication(med.name, med.dose, labDate, affectedOrgansArray, abnormalDescriptions);

    // CHANGE: Check if medication should be skipped in response
    if (result._skipInResponse) {
      console.log(`[REQUEST] ⚠ Skipping "${med.name}" from response - ${result.reason}`);
      skippedMedications.push({
        name: med.name,
        reason: result.reason,
        drugClass: result.drugClass,
        message: result.message
      });
    } else {
      results.push(result);
    }
  }

  const response = {
    medicationList: results
  };

  // CHANGE: Add skipped medications info if any
  if (skippedMedications.length > 0) {
    response.skippedMedications = skippedMedications;
    console.log(`\n[RESPONSE] ${skippedMedications.length} medication(s) skipped and added to database`);
  }

  console.log('\n' + '='.repeat(80));
  console.log('[RESPONSE] Final Response:');
  console.log('='.repeat(80));
  console.log(JSON.stringify(response, null, 2));
  console.log('='.repeat(80) + '\n');

  return response;
}

function createBlankResponse(name, dose, date, reason = 'not found', geminiResult = null, abnormalBiomarkers = [], filteredOrgans = '') {
  console.log(`[RESPONSE] Creating blank response - Reason: ${reason}`);
  return {
    name: name || '',
    dose: dose || '',
    date: date || '',
    drugCategory: '',
    drugClass: geminiResult?.actualDrugClass || 'not found',
    cautionHeadline: '',
    cautionNote: '',
    icd10: '',
    snomed: '',
    ahaLabTriggerOrgans: filteredOrgans,
    biomarkerAbnormal: '',
    matchType: 'no_match',
    matchReason: reason,
    ruleHit: null,
    geminiAnalysis: geminiResult || null,
  };
}

function formatResponse(name, dose, date, rule, matchType, geminiResult = null, filteredOrgans = '', filteredAbnormals = []) {
  console.log(`[RESPONSE] Formatting response - Match Type: ${matchType}`);
  console.log(`[RESPONSE] Filtered organs: "${filteredOrgans}"`);
  console.log(`[RESPONSE] Filtered abnormals: [${filteredAbnormals.join(', ')}]`);

  return {
    name,
    dose: dose || '',
    date: date || '',
    // drugCategory: rule.drugCategory,
    drugClass: rule.drugClass,
    // cautionHeadline: rule.cautionHeadline,
    cautionNote: rule.cautionNote,
    icd10: rule.icd10,
    snomed: rule.snomed,
    ahaLabTriggerOrgans: filteredOrgans,
    biomarkerAbnormal: filteredAbnormals.join(', '),
    matchType,
    ruleHit: rule.rowIndex,
    basedOn: rule.basedOn,
    geminiAnalysis: geminiResult || null,
  };
}

const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

async function extractPdfText(buffer) {
  const loadingTask = PDFJS.getDocument({ data: new Uint8Array(buffer) });
  const pdf = await loadingTask.promise;
  const pageCount = pdf.numPages;
  const texts = [];

  for (let i = 1; i <= pageCount; i++) {
    const page = await pdf.getPage(i);
    const content = await page.getTextContent();
    const pageText = content.items.map(item => item.str).join(' ');
    texts.push(pageText);
  }

  return texts.join('\n\n');
}

// Code2 functions and classes
function createConcurrencyLimiter(maxConcurrent) {
  let running = 0;
  const queue = [];

  return async function limit(fn) {
    while (running >= maxConcurrent) {
      await new Promise(resolve => queue.push(resolve));
    }
    running++;
    try {
      return await fn();
    } finally {
      running--;
      const resolve = queue.shift();
      if (resolve) resolve();
    }
  };
}

class PerfLogger {
  constructor(id) {
    this.id = id;
    this.startTime = Date.now();
    this.marks = {};
  }

  mark(label) {
    const now = Date.now();
    const elapsed = now - this.startTime;
    const lastMark = Object.keys(this.marks).length > 0 
      ? now - (this.startTime + Object.values(this.marks)[Object.keys(this.marks).length - 1])
      : elapsed;
    
    this.marks[label] = elapsed;
    console.log(`⏱️  [${this.id}] ${label}: ${elapsed}ms (Δ +${lastMark}ms)`);
  }

  summary() {
    const total = Date.now() - this.startTime;
    console.log(`📊 [${this.id}] TOTAL TIME: ${total}ms`);
    return total;
  }
}

function createBlankResponseForBiomarkers() {
  const blank = {};
  for (const key in fullBiomarkerList) {
    blank[key] = null;
  }
  return {
    finalBiomarkerValues: blank,
    finalConfidenceValues: { ...blank },
    finalUnitValues: { ...blank },
    labProvider: null,
    labCollectedDate: null
  };
}

function cleanAndExtractBiomarkers(fullText) {
  return fullText.trim();
}

async function extractPdfTextAdvanced(buffer, perf) {
  perf.mark('PDF_START');
  
  try {
    const pdf = await PDFJS.getDocument({ data: new Uint8Array(buffer) }).promise;
    perf.mark('PDF_LOADED');
    
    const pageCount = pdf.numPages;
    console.log(`📄 PDF has ${pageCount} pages, extracting ALL pages in parallel`);

    const pagePromises = [];
    for (let i = 1; i <= pageCount; i++) {
      pagePromises.push(
        pdf.getPage(i).then(page => 
          page.getTextContent().then(content => 
            content.items.map(item => item.str).join(' ')
          )
        )
      );
    }

    const texts = await Promise.all(pagePromises);
    perf.mark('PDF_EXTRACTED');
    
    const fullText = texts.join('\n\n');
    const cleanedText = cleanAndExtractBiomarkers(fullText);
    
    console.log(`📊 Full: ${fullText.length} chars → Cleaned: ${cleanedText.length} chars`);
    
    return cleanedText;
  } catch (error) {
    console.error('❌ Error extracting PDF:', error.message);
    throw error;
  }
}

async function extractImageText(buffer, perf) {
  perf.mark('OCR_START');
  
  try {
    console.log('🖼 Starting OCR...');
    const result = await Promise.race([
      Tesseract.recognize(buffer, 'eng'),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('OCR timeout')), 30000)
      )
    ]);
    
    perf.mark('OCR_COMPLETED');
    const text = result?.data?.text || '';
    const cleanedText = cleanAndExtractBiomarkers(text);
    console.log(`✅ OCR: ${text.length} chars → Cleaned: ${cleanedText.length} chars`);
    return cleanedText;
  } catch (error) {
    console.error('❌ OCR failed:', error.message);
    throw error;
  }
}

function sanitizeValue(rawValue) {
  if (!rawValue) return null;
  if (typeof rawValue !== 'string') return rawValue.toString();
  
  const trimmed = rawValue.trim();
  if (trimmed.toLowerCase() === 'negative') return trimmed;
  
  const num = parseFloat(trimmed);
  return !isNaN(num) ? num.toString() : null;
}

function remapKeys(obj, altNames) {
  const remapped = {};
  for (const [key, value] of Object.entries(obj)) {
    const mapped = altNames[key.toLowerCase().trim()] || key.trim();
    remapped[mapped] = value;
  }
  return remapped;
}

// Process Gemini response
function processGeminiResponse(geminiText, perf) {
  perf.mark('PARSE_GEMINI_START');
  
  const cleanedText = geminiText
    .replace(/```json/g, '')
    .replace(/```/g, '')
    .trim();

  const jsonBlocks = cleanedText
    .split(/\n\s*\n|\n(?=\{)/)
    .map(block => block.trim())
    .filter(block => block);

  while (jsonBlocks.length < 5) jsonBlocks.push('{}');

  const results = [{}, {}, {}, {}, {}];
  
  for (let i = 0; i < 5; i++) {
    try {
      results[i] = JSON.parse(jsonBlocks[i]);
    } catch (e) {
      console.warn(`⚠️  JSON block ${i} parse failed`);
    }
  }

  perf.mark('PARSE_GEMINI_END');
  return results;
}

// Improved Gemini prompt
function buildGeminiPrompt(extractedText) {
  return `You are a medical lab report analyzer. Extract ALL biomarkers AND metadata from this blood test report.

Return ONLY 5 JSON objects (one per line), no other text:

1. BIOMARKER VALUES: {"biomarker_name": numerical_value_only, ...}
2. CONFIDENCE: {"biomarker_name": "CONFIDENT", ...}
3. UNITS: {"biomarker_name": "unit_string", ...}
4. LAB PROVIDER: {"labProvider": "LabCorp" or "Quest" or "Other"}
5. COLLECTION DATE: {"labCollectedDate": "MM/DD/YYYY"}

IMPORTANT FOR BIOMARKERS:
- Extract ALL biomarkers mentioned in the report
- Include both current and historical values
- Do NOT include patient names or dates as biomarkers
- For values with '<' or '>', convert to approximate number
- Keep exact names from the report, don't abbreviate

IMPORTANT FOR METADATA:
- Lab Provider: Look for "Labcorp", "LabCorp", "Quest", "Quest Diagnostics" in headers/footers
- Collection Date: Look for "Date Collected:", "Collection Date:", "Collected:" followed by a date
- Use exact format MM/DD/YYYY for the date
- If lab provider is not clearly Labcorp or Quest, use "Other"

Blood Test Report:
${extractedText}

Return exactly 5 lines of JSON, nothing else.`;
}

async function callGeminiAPI(prompt, perf, retries = 2) {
  const geminiApiKey = process.env.GEMINI_API_KEY;
  if (!geminiApiKey) throw new Error("GEMINI_API_KEY missing");

  perf.mark('GEMINI_API_CALL_START');

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await axios.post(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${geminiApiKey}`,
        { 
          contents: [{ 
            parts: [{ text: prompt }] 
          }]
        },
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );

      perf.mark('GEMINI_API_CALL_END');
      return response.data?.candidates?.[0]?.content?.parts?.[0]?.text || '{}\n{}\n{}\n{}\n{}';
    } catch (error) {
      console.warn(`⚠️  Gemini API attempt ${attempt + 1} failed: ${error.message}`);
      if (attempt === retries) throw error;
      await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1))); // Exponential backoff
    }
  }
}

async function processExtraction(fullExtractedText, perf) {
  if (!fullExtractedText || fullExtractedText.trim().length < 20) {
    console.warn('⚠️  Empty or minimal text extracted');
    return createBlankResponseForBiomarkers();
  }

  try {
    perf.mark('EXTRACTION_START');
    
    console.log(`📄 Processing ${fullExtractedText.length} characters of extracted text`);

    const prompt = buildGeminiPrompt(fullExtractedText);
    perf.mark('PROMPT_READY');

    console.log('🚀 Calling Gemini API with full text...');
    const geminiText = await callGeminiAPI(prompt, perf);
    
    console.log(`✅ Gemini response length: ${geminiText.length} chars`);
    
    const [biomarkerJson, confidenceJson, unitJson, labProviderJson, labDateJson] = processGeminiResponse(geminiText, perf);

    console.log(`📊 Parsed biomarkers: ${Object.keys(biomarkerJson).length}`);
    console.log(`📊 Parsed confidence: ${Object.keys(confidenceJson).length}`);
    console.log(`📊 Parsed units: ${Object.keys(unitJson).length}`);
    console.log(`📊 Lab Provider: ${JSON.stringify(labProviderJson)}`);
    console.log(`📊 Lab Date: ${JSON.stringify(labDateJson)}`);

    perf.mark('REMAP_START');
    const parsedBiomarkerData = remapKeys(biomarkerJson, altNames);
    const parsedConfidenceData = remapKeys(confidenceJson, altNames);
    const parsedUnitData = remapKeys(unitJson, altNames);
    perf.mark('REMAP_END');

    perf.mark('INIT_FINAL_OBJECTS');
    const finalBiomarkerValues = {};
    const finalConfidenceValues = {};
    const finalUnitValues = {};

    for (const key in fullBiomarkerList) {
      finalBiomarkerValues[key] = null;
      finalConfidenceValues[key] = null;
      finalUnitValues[key] = null;
    }

    perf.mark('POPULATE_START');
    
    for (const [key, value] of Object.entries(parsedBiomarkerData)) {
      if (key in finalBiomarkerValues) {
        finalBiomarkerValues[key] = sanitizeValue(value);
      }
    }

    for (const [key, value] of Object.entries(parsedConfidenceData)) {
      if (key in finalConfidenceValues) {
        finalConfidenceValues[key] = value ? value.toString() : null;
      }
    }

    for (const [key, value] of Object.entries(parsedUnitData)) {
      if (key in finalUnitValues) {
        finalUnitValues[key] = value || null;
      }
    }

    if ((finalBiomarkerValues["Eosinophil"] === "0" || finalBiomarkerValues["Eosinophil"] === "0.0") &&
        parseFloat(finalBiomarkerValues["% Eosinophil"]) > 0) {
      finalBiomarkerValues["Eosinophil"] = "30";
    }

    if ((finalBiomarkerValues["Basophil"] === "0" || finalBiomarkerValues["Basophil"] === "0.0") &&
        parseFloat(finalBiomarkerValues["% Basophil"]) > 0) {
      finalBiomarkerValues["Basophil"] = "30";
    }

    perf.mark('POPULATE_END');

    const filteredBiomarkers = Object.fromEntries(
      Object.entries(finalBiomarkerValues).filter(([_, value]) =>
        value !== null && value !== undefined && value !== ''
      )
    );

    // Extract metadata
    const labProvider = labProviderJson.labProvider || null;
    const labCollectedDate = labDateJson.labCollectedDate || null;

    return { 
      filteredBiomarkers,
      labProvider,
      labCollectedDate
    };

  } catch (error) {
    console.error('❌ Extraction error:', error.message);
    return createBlankResponseForBiomarkers();
  }
}

async function processSinglePdf(file, fileIndex) {
  const perf = new PerfLogger(`FILE_${fileIndex}_${file.originalname}`);
  
  try {
    perf.mark('START');
    const filePath = path.resolve(file.path);
    
    perf.mark('READ_FILE_START');
    const fileBuffer = await fs1.readFile(filePath);
    perf.mark('READ_FILE_END');

    let fullExtractedText = '';

    // Extract text
    if (file.originalname.toLowerCase().endsWith('.pdf')) {
      fullExtractedText = await extractPdfTextAdvanced(fileBuffer, perf);
    } else {
      fullExtractedText = await extractImageText(fileBuffer, perf);
    }

    const cleanupPromise = fs1.unlink(filePath).catch(() => {});

    const result = await processExtraction(fullExtractedText, perf);

    await cleanupPromise;
    perf.mark('CLEANUP_END');

    perf.summary();

    return {
      fileName: file.originalname,
      success: true,
      ...result
    };

  } catch (err) {
    console.error(`❌ Error processing ${file.originalname}:`, err.message);
    await fs1.unlink(path.resolve(file.path)).catch(() => {});
    
    perf.summary();

    return {
      fileName: file.originalname,
      success: false,
      error: err.message
    };
  }
}

// Endpoints

// /test endpoint from code1
app.post('/test', async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    console.log('[REQUEST] ✗ Missing or invalid Authorization header');
    return res.status(401).json({ error: 'Bearer token required in Authorization header' });
  }

  const bearerToken = authHeader.substring(7);

  try {
    const response = await processTestRequest(req.body, bearerToken);
    res.json(response);
  } catch (error) {
    console.error('\n[ERROR] Request processing failed:', error.message);
    console.error(error.stack);

    res.status(500).json({
      error: 'Failed to process request',
      message: error.message,
      details: error.response?.data || null
    });
  }
});

// /upload endpoint from code1 (Lambda-based)
app.post('/upload', uploadSingle.single('pdf'), async (req, res) => {
  try {
    console.log('✅ /upload API hit');

    const file = req.file;
    if (!file) {
      return res.status(400).json({ error: 'No file uploaded.' });
    }

    const filePath = path.resolve(file.path);
    const fileBuffer = await fs1.readFile(filePath);

    let fullExtractedText = '';

    // --- Extract text from PDF or image ---
    if (file.originalname.toLowerCase().endsWith('.pdf')) {
      console.log('📄 Extracting text from PDF...');
      const pdfText = await extractPdfText(fileBuffer);
      fullExtractedText = pdfText;
    } else {
      console.log('🖼 Extracting text from image...');
      const { data: { text: imgText } } = await Tesseract.recognize(fileBuffer, 'eng');
      fullExtractedText = imgText;
    }

    // --- Prepare and send Lambda request ---
    console.log('🚀 Sending text to Lambda for processing...');
    const command = new InvokeCommand({
      FunctionName: 'processGeminiExtraction',
      Payload: Buffer.from(JSON.stringify({
        body: JSON.stringify({ fullExtractedText }),
      })),
    });

    const lambdaResult = await lambdaClient.send(command);
    const payloadString = new TextDecoder().decode(lambdaResult.Payload);
    const payload = JSON.parse(payloadString);

    console.log('🧠 Lambda response received:', payload);

    if (payload.statusCode !== 200) {
      const errorData = JSON.parse(payload.body);
      throw new Error(errorData.message || errorData.error || 'Lambda error');
    }

    const { finalBiomarkerValues } = JSON.parse(payload.body);

    // Clean up the uploaded file
    await fs1.unlink(filePath);

    console.log('✅ Final Biomarker Values:', finalBiomarkerValues);

    // --- Send response back to frontend ---
    return res.status(200).json({
      message: 'File processed successfully.',
      finalBiomarkerValues,
    });

  } catch (err) {
    console.error('❌ Error in /upload:', err);
    return res.status(500).json({
      error: err.message || 'Internal server error',
    });
  }
});

// /upload-multi endpoint from code2 (local processing, internal call to processTestRequest)
app.post('/upload-multi', uploadMulti.array('pdfs', 10), async (req, res) => {
  const globalPerf = new PerfLogger('UPLOAD_MULTI');
  
  try {
    globalPerf.mark('API_HIT');

    const files = req.files;
    if (!files || files.length === 0) {
      return res.status(400).json({ error: 'No files uploaded.' });
    }

    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Bearer token required in Authorization header' });
    }
    const bearerToken = authHeader.substring(7); 

    console.log(`\n📦 ========================================`);
    console.log(`📦 Processing ${files.length} file(s) in FULL PARALLEL...`);
    console.log(`📦 ========================================\n`);

    globalPerf.mark('FILES_VALIDATED');

    const limit = createConcurrencyLimiter(5); 
    
    const processingPromises = files.map((file, idx) => 
      limit(() => processSinglePdf(file, idx + 1))
    );

    const results = await Promise.all(processingPromises);

    globalPerf.mark('ALL_FILES_PROCESSED');

    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);

    // Merge logic for latest biomarkers
    let latestBiomarkers = {};
    let latestLabProvider = null;
    let latestLabCollectedDate = null;

    if (successful.length > 0) {
      const successfulWithDates = successful
        .map(r => {
          if (!r.labCollectedDate) return null;
          const parts = r.labCollectedDate.split('/');
          if (parts.length !== 3) return null;
          const month = parseInt(parts[0], 10);
          const day = parseInt(parts[1], 10);
          const year = parseInt(parts[2], 10);
          if (isNaN(month) || isNaN(day) || isNaN(year)) return null;
          const date = new Date(year, month - 1, day);
          if (isNaN(date.getTime())) return null;
          return { date, biomarkers: r.filteredBiomarkers, provider: r.labProvider };
        })
        .filter(Boolean);

      if (successfulWithDates.length > 0) {
        // Track latest value per biomarker
        const biomarkerHistory = new Map();
        for (const { date, biomarkers, provider } of successfulWithDates) {
          for (const [key, value] of Object.entries(biomarkers)) {
            const existing = biomarkerHistory.get(key);
            if (!existing || date > existing.date) {
              biomarkerHistory.set(key, { value, date });
            }
          }
        }

        latestBiomarkers = Object.fromEntries(
          Array.from(biomarkerHistory.entries()).map(([key, { value }]) => [key, value])
        );

        // Find max date
        const maxDate = new Date(Math.max(...successfulWithDates.map(r => r.date)));
        latestLabCollectedDate = maxDate.toLocaleDateString('en-US');

        // Find provider from a report on max date (take first)
        const latestReport = successfulWithDates.find(r => r.date.getTime() === maxDate.getTime());
        latestLabProvider = latestReport ? latestReport.provider : null;
      } else {
        console.warn('⚠️ No valid dates found in successful reports; using blank merged result.');
      }
    }

    globalPerf.mark('RESPONSE_READY');

    // REMOVED: Internal call to biomarker analysis (/test) - Frontend now calls /test separately with dynamic user inputs
    // (Previously hardcoded sex="male" and static medications; now dynamic via frontend)
    const medicationResult = null; // Not computed here

    globalPerf.mark('ALL_PROCESSING_COMPLETE');
    globalPerf.summary();

    console.log(`\n✅ ========================================`);
    console.log(`✅ Success: ${successful.length} | Failed: ${failed.length}`);
    console.log(`✅ Latest Biomarkers Count: ${Object.keys(latestBiomarkers).length}`);
    console.log(`✅ Total Time: ${globalPerf.marks['ALL_PROCESSING_COMPLETE']}ms`);
    console.log(`✅ ========================================\n`);

    return res.status(200).json({
      message: 'Files processed.',
      totalFiles: files.length,
      successfulCount: successful.length,
      failedCount: failed.length,
      totalTimeMs: globalPerf.marks['ALL_PROCESSING_COMPLETE'],
      latestBiomarkers,  // Used as 'biomarkers' in /test call
      latestLabProvider,
      latestLabCollectedDate,  // Used as 'labDate' in /test call
      // REMOVED: "medicationResult" - Now handled by separate /test call in frontend
    });

  } catch (err) {
    console.error('❌ Error in /upload-multi:', err);
    globalPerf.summary();
    return res.status(500).json({
      error: err.message || 'Internal server error'
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log('\n' + '█'.repeat(80));
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█' + `  API Server Running on http://localhost:${PORT}`.padEnd(78) + '█');
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█'.repeat(80) + '\n');
});