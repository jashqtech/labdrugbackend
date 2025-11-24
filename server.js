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
const app = express();
const PORT = 3000;

app.use(express.json());
const upload = multer({ dest: 'uploads/' }); // Files go into "uploads/" folder

app.use(cors());
let rules = [];
const CSV_HEADERS = [
  'Based_On',
  'Drug Category',
  'Drug Class',
  'Medications',
  'AHA Lab Trigger - Organs',
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
      const icd10 = r['ICD-10 Diagnostic Code']?.trim() || '';
      const snomed = r['SNOMED']?.trim() || '';
      const ahaLabTriggerOrgans = r['AHA Lab Trigger - Organs']?.trim() || '';
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
        cautionNote,
        icd10,
        snomed,
        ahaLabTriggerOrgans,
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
    return '';
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
        affectedOrgans.push(`${status} ${organName.toLowerCase()}`);
      }
    }

    const result = affectedOrgans.join(', ');
    console.log(`[ORGAN PROCESSING] Final affected organs: "${result}"`);
    return result;

  } catch (error) {
    console.error('[ORGAN PROCESSING] ERROR parsing organ data:', error.message);
    return '';
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
      'AHA Lab Trigger - Organs': '',
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
    const fullDesc = desc; // Keep original casing

    if (included.has(fullDesc)) continue; // Already included

    let shouldInclude = false;

    // Check abnormal triggers (any direction)
    if (abnormalTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
    }
    // Check low triggers
    else if (direction === 'low' && lowTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
    }
    // Check high triggers
    else if (direction === 'high' && highTriggers.some(trigger => biomarkerName.includes(trigger) || trigger.includes(biomarkerName))) {
      shouldInclude = true;
    }

    if (shouldInclude) {
      filtered.push(fullDesc);
      included.add(fullDesc);
      console.log(`[FILTER BIOMARKERS] ✓ Included: ${fullDesc} (direction: ${direction}, biomarker: ${biomarkerName})`);
    } else {
      console.log(`[FILTER BIOMARKERS] ○ Excluded: ${fullDesc}`);
    }
  }

  console.log(`[FILTER BIOMARKERS] Filtered down to ${filtered.length} matching abnormals`);
  return filtered;
}

async function processMedication(medName, dose, date, affectedOrgans = '', abnormalBiomarkers = []) {
  console.log('\n' + '='.repeat(80));
  console.log(`[PROCESSING] Medication: "${medName}"`);
  console.log(`[PROCESSING] Affected Organs from Lab: "${affectedOrgans}"`);
  console.log(`[PROCESSING] Total Abnormal Biomarkers: ${abnormalBiomarkers.length}`);
  console.log('='.repeat(80));

  if (!medName || typeof medName !== 'string') {
    console.log('[PROCESSING] ✗ Invalid medication name');
    return createBlankResponse(medName, dose, date, affectedOrgans, 'Invalid name', null, []);
  }

  let matchedRule = exactMedicationMatch(medName);

  if (matchedRule) {
    console.log(`[PROCESSING] ✓ STEP 1 succeeded - exact match found`);
    const filteredAbnormals = filterAbnormalBiomarkers(abnormalBiomarkers, matchedRule);
    return formatResponse(medName, dose, date, matchedRule, 'exact_match', null, affectedOrgans, filteredAbnormals);
  }

  console.log(`[PROCESSING] Proceeding to STEP 2 - Gemini API lookup`);
  const geminiResult = await identifyDrugClassWithGemini(medName);

  if (!geminiResult) {
    console.log('[PROCESSING] ✗ STEP 2 failed - Gemini API error');
    return createBlankResponse(medName, dose, date, affectedOrgans, 'Gemini API error', null, []);
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
    return createBlankResponse(medName, dose, date, affectedOrgans, 'Drug class not identified', null, []);
  }

  console.log(`[PROCESSING] Proceeding to STEP 3 - matching drug class to rules`);
  matchedRule = matchDrugClassToRule(drugClassToMatch, medName);

  if (!matchedRule) {
    console.log('[PROCESSING] ✗ STEP 3 failed - no matching rule found');
    return createBlankResponse(medName, dose, date, affectedOrgans, `Drug class "${drugClassToMatch}" not found in rules`, geminiResult, []);
  }

  console.log(`[PROCESSING] ✓ STEP 3 succeeded - matched to row ${matchedRule.rowIndex}`);

  addMedicationToCSV(matchedRule.drugClass, medName);

  const filteredAbnormals = filterAbnormalBiomarkers(abnormalBiomarkers, matchedRule);
  return formatResponse(medName, dose, date, matchedRule, 'gemini_match', geminiResult, affectedOrgans, filteredAbnormals);
}

app.post('/test', async (req, res) => {
  console.log('\n\n' + '█'.repeat(80));
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█' + '  NEW REQUEST RECEIVED'.padEnd(78) + '█');
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█'.repeat(80) + '\n');
  console.log('[REQUEST] Body:', JSON.stringify(req.body, null, 2));

  const {
    labDate,
    patientSex,
    biomarkers,
    medicationList,
    organizationId,
    patientId
  } = req.body;

  if (!labDate || !patientSex || !biomarkers || !medicationList || !organizationId || !patientId) {
    console.log('[REQUEST] ✗ Missing required fields');
    return res.status(400).json({
      error: 'Required fields: labDate, patientSex, biomarkers, medicationList, organizationId, patientId'
    });
  }

  if (!Array.isArray(medicationList) || !medicationList.length) {
    console.log('[REQUEST] ✗ Invalid request - medicationList must be a non-empty array');
    return res.status(400).json({ error: 'medicationList must be a non-empty array' });
  }

  console.log(`[REQUEST] Lab Date: ${labDate}`);
  console.log(`[REQUEST] Patient Sex: ${patientSex}`);
  console.log(`[REQUEST] Organization ID: ${organizationId}`);
  console.log(`[REQUEST] Patient ID: ${patientId}`);
  console.log(`[REQUEST] Processing ${medicationList.length} medication(s)`);

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    console.log('[REQUEST] ✗ Missing or invalid Authorization header');
    return res.status(401).json({ error: 'Bearer token required in Authorization header' });
  }

  const bearerToken = authHeader.substring(7);
  console.log(`[REQUEST] Bearer token extracted: ${bearerToken.substring(0, 20)}...`);

  try {
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
    const affectedOrgans = processOrganData(organDataString);
    console.log(`[REQUEST] Affected organs to use for medications: "${affectedOrgans}"`);

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

      const result = await processMedication(med.name, med.dose, labDate, affectedOrgans, abnormalDescriptions);

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
      //   labResultsData: {
      //     id: labResultsResponse.id,
      //     maxScore: labResultsResponse.MaxScore,
      //     maxScoreOrgan: labResultsResponse.MaxScoreOrgan,
      //     organData: organDataString,
      //     affectedOrgans: affectedOrgans,
      //     prescriptionLink: labResultsResponse.PrescriptionLink,
      //     recommendations: labResultsResponse.Recommendations,
      //     resultStatus: labResultsResponse.ResultStatus,
      //     insightsDiet: labResultsResponse.InsightsDiet,
      //     insightsHydration: labResultsResponse.InsightsHydration,
      //     insightsRest: labResultsResponse.InsightsRest
      //   },
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






function createBlankResponse(name, dose, date, affectedOrgans = '', reason = 'not found', geminiResult = null, abnormalBiomarkers = []) {
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
    ahaLabTriggerOrgans: affectedOrgans, // Use processed organ data
    biomarkerAbnormal: '',
    matchType: 'no_match',
    matchReason: reason,
    ruleHit: null,
    geminiAnalysis: geminiResult || null,
  };
}

function formatResponse(name, dose, date, rule, matchType, geminiResult = null, affectedOrgans = '', filteredAbnormals = []) {
  console.log(`[RESPONSE] Formatting response - Match Type: ${matchType}`);
  console.log(`[RESPONSE] Using affected organs: "${affectedOrgans}"`);
  console.log(`[RESPONSE] Filtered abnormals: [${filteredAbnormals.join(', ')}]`);

  return {
    name,
    dose: dose || '',
    date: date || '',
    // drugCategory: rule.drugCategory,
    drugClass: rule.drugClass,
    cautionHeadline: '',
    cautionNote: rule.cautionNote,
    icd10: rule.icd10,
    snomed: rule.snomed,
    ahaLabTriggerOrgans: affectedOrgans || rule.ahaLabTriggerOrgans,
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
// Helper: check if file is PDF
function isPDF(filename) {
  return filename.toLowerCase().endsWith('.pdf');
}


app.post('/upload', upload.single('pdf'), async (req, res) => {
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




app.listen(PORT, () => {
  console.log('\n' + '█'.repeat(80));
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█' + `  API Server Running on http://localhost:${PORT}`.padEnd(78) + '█');
  console.log('█' + ' '.repeat(78) + '█');
  console.log('█'.repeat(80) + '\n');
});