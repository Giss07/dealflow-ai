function doPost(e) {
  try {
    var sheet = SpreadsheetApp.openById("1GMp9LbZLgY_uaTjiDQ9cTcy4I1QxOqLsNZWORwkUMCY");
    var tab = sheet.getSheetByName("Properties_Offer_Tracker_Template");
    if (!tab) {
      return ContentService.createTextOutput(JSON.stringify({error: "Tab not found"})).setMimeType(ContentService.MimeType.JSON);
    }
    var data = JSON.parse(e.postData.contents);
    var dateStr = data.date || "";
    if (dateStr && dateStr.indexOf("-") > -1) {
      var parts = dateStr.split("-");
      dateStr = parts[1] + "/" + parts[2] + "/" + parts[0];
    }
    var address = data.address || "";
    var zillowUrl = "https://www.zillow.com/homes/" + encodeURIComponent(address) + "_rb/";
    var arvVal = data.arv ? Number(data.arv).toLocaleString("en-US") : "";
    var offerVal = data.offer_amount ? Number(data.offer_amount).toLocaleString("en-US") : "";
    var repairsVal = data.repairs ? Number(data.repairs).toLocaleString("en-US") : "";
    tab.appendRow([dateStr, "DealFlow AI", "", arvVal, offerVal, repairsVal, "", "", "", "", "", "", "", "", "", data.arv_justification || ""]);
    var lastRow = tab.getLastRow();
    var safeAddr = address.replace(/"/g, '""');
    tab.getRange(lastRow, 3).setFormula('=HYPERLINK("' + zillowUrl + '", "' + safeAddr + '")');
    var compsRaw = data.comps || "";
    var compsArr = compsRaw.split(" | ");
    var validComps = [];
    for (var c = 0; c < compsArr.length; c++) {
      if (compsArr[c].trim()) {
        validComps.push(compsArr[c].trim());
      }
    }
    if (validComps.length > 0) {
      var formulaParts = [];
      for (var j = 0; j < validComps.length; j++) {
        var compAddr = validComps[j].replace(/"/g, '""');
        var compUrl = "https://www.zillow.com/homes/" + encodeURIComponent(validComps[j]) + "_rb/";
        formulaParts.push('HYPERLINK("' + compUrl + '", "' + compAddr + '")');
      }
      tab.getRange(lastRow, 15).setFormula("=" + formulaParts.join(' & CHAR(10) & '));
      tab.getRange(lastRow, 15).setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);
    }
    return ContentService.createTextOutput(JSON.stringify({result: "ok"})).setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({error: err.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  try {
    var sheet = SpreadsheetApp.openById("1GMp9LbZLgY_uaTjiDQ9cTcy4I1QxOqLsNZWORwkUMCY");
    var tab = sheet.getSheetByName("Properties_Offer_Tracker_Template");
    if (!tab) {
      return ContentService.createTextOutput(JSON.stringify({error: "Tab not found"})).setMimeType(ContentService.MimeType.JSON);
    }
    var data = tab.getDataRange().getDisplayValues();
    if (data.length < 2) {
      return ContentService.createTextOutput(JSON.stringify({rows: []})).setMimeType(ContentService.MimeType.JSON);
    }
    var rows = [];
    for (var i = 1; i < data.length; i++) {
      var r = data[i];
      if (!r[2] && !r[0]) {
        continue;
      }
      var row = {
        date: r[0] || "",
        source: r[1] || "",
        address: r[2] || "",
        arv: r[3] || "",
        offer: r[4] || "",
        repairs: r[5] || "",
        closing_date: r[6] || "",
        inspection: r[7] || "",
        emd: r[8] || "",
        sent_date: r[9] || "",
        status: r[10] || "",
        counter_price: r[11] || "",
        counter_date: r[12] || "",
        notes: r[13] || "",
        comps: r[14] || "",
        justification: r[15] || ""
      };
      rows.push(row);
    }
    return ContentService.createTextOutput(JSON.stringify({rows: rows})).setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({error: err.toString()})).setMimeType(ContentService.MimeType.JSON);
  }
}
