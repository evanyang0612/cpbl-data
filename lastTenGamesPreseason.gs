function mapLatestPreseasonGamesByDate() {
  // Helper function to transform text values
  function transformText(text) {
    if (typeof text !== "string") return text;
    var nameMap = {
      "統一7-ELEVEn": "統一",
      中信兄弟: "兄弟",
    };
    if (nameMap.hasOwnProperty(text)) {
      text = nameMap[text];
    }
    if (text.length === 2) {
      text = text.charAt(0) + " " + text.charAt(1);
    }
    return text;
  }

  // Open the spreadsheet and get sheets.
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sourceSheet = ss.getSheetByName("熱身賽賽程");
  var targetSheet = ss.getSheetByName("近十場");
  var settingSheet = ss.getSheetByName("設定");

  // Get all source data (assuming the first row is headers)
  var sourceData = sourceSheet.getDataRange().getValues();
  var headers = sourceData[0];

  // Define header names (adjust if your actual headers differ)
  var homeTeamHeader = "主隊";
  var awayTeamHeader = "客隊";
  var homePitcherHeader = "主隊先發";
  var awayPitcherHeader = "客隊先發";
  var dateHeader = "日期";
  var park = "球場";
  var homeTeamEarnedRun = "主總自責分";
  var awayTeamEarnedRun = "客總自責分";
  var homeTeamRun = "主得分";
  var awayTeamRun = "客得分";
  var homeHits = "主總安打";
  var awayHits = "客總安打";
  var homeHomerun = "主全壘打";
  var awayHomerun = "客全壘打";
  var homeWalk = "主四壞球";
  var homeDeadball = "主死球";
  var awayWalk = "客四壞球";
  var awayDeadball = "客死球";
  var homeStrikedOut = "主被三振";
  var awayStrikedOut = "客被三振";

  // Get the column indexes (0-indexed) for each header
  var homeTeamIndex = headers.indexOf(homeTeamHeader);
  var awayTeamIndex = headers.indexOf(awayTeamHeader);
  var homePitcherIndex = headers.indexOf(homePitcherHeader);
  var awayPitcherIndex = headers.indexOf(awayPitcherHeader);
  var dateIndex = headers.indexOf(dateHeader);
  var parkIndex = headers.indexOf(park);
  var homeTeamEarnedRunIndex = headers.indexOf(homeTeamEarnedRun);
  var awayTeamEarnedRunIndex = headers.indexOf(awayTeamEarnedRun);
  var homeTeamRunIndex = headers.indexOf(homeTeamRun);
  var awayTeamRunIndex = headers.indexOf(awayTeamRun);
  var homeHitsIndex = headers.indexOf(homeHits);
  var awayHitsIndex = headers.indexOf(awayHits);
  var homeHomerunIndex = headers.indexOf(homeHomerun);
  var awayHomerunIndex = headers.indexOf(awayHomerun);
  var homeWalkIndex = headers.indexOf(homeWalk);
  var homeDeadballIndex = headers.indexOf(homeDeadball);
  var awayWalkIndex = headers.indexOf(awayWalk);
  var awayDeadballIndex = headers.indexOf(awayDeadball);
  var homeStrikedOutIndex = headers.indexOf(homeStrikedOut);
  var awayStrikedOutIndex = headers.indexOf(awayStrikedOut);

  if (
    homeTeamIndex === -1 ||
    awayTeamIndex === -1 ||
    homePitcherIndex === -1 ||
    awayPitcherIndex === -1 ||
    dateIndex === -1 ||
    parkIndex === -1 ||
    homeTeamEarnedRunIndex === -1 ||
    awayTeamEarnedRunIndex === -1
  ) {
    throw new Error(
      "One or more required headers not found. Please check your header names.",
    );
  }

  // Only include games from the current year.
  var targetYear = new Date().getFullYear();

  // Object to store each team's games.
  var teamGames = {};

  // Loop through each row (skipping the header row)
  for (var i = 1; i < sourceData.length; i++) {
    var row = sourceData[i];
    var gameDate = new Date(row[dateIndex]);

    // Only include data for 2026.
    if (gameDate.getFullYear() !== targetYear) {
      continue;
    }

    // For the home team:
    var homeTeam = row[homeTeamIndex];
    if (homeTeam) {
      if (!teamGames[homeTeam]) {
        teamGames[homeTeam] = [];
      }
      teamGames[homeTeam].push({
        opponent: row[awayTeamIndex],
        opposingPitcher: row[awayPitcherIndex],
        date: gameDate,
        park: row[parkIndex],
        realPointsEarned: row[awayTeamEarnedRunIndex],
        realPointsLost: row[homeTeamEarnedRunIndex],
        pointsEarned: row[homeTeamRunIndex],
        hits: row[homeHitsIndex],
        pointsLost: row[awayTeamRunIndex],
        homerun: row[homeHomerunIndex],
        strikeOut: row[homeStrikedOutIndex],
        walkWithDeadball: row[homeWalkIndex] + row[homeDeadballIndex],
      });
    }

    // For the away team:
    var awayTeam = row[awayTeamIndex];
    if (awayTeam) {
      if (!teamGames[awayTeam]) {
        teamGames[awayTeam] = [];
      }
      teamGames[awayTeam].push({
        opponent: row[homeTeamIndex],
        opposingPitcher: row[homePitcherIndex],
        date: gameDate,
        park: row[parkIndex],
        realPointsEarned: row[homeTeamEarnedRunIndex],
        realPointsLost: row[awayTeamEarnedRunIndex],
        pointsEarned: row[awayTeamRunIndex],
        hits: row[awayHitsIndex],
        pointsLost: row[homeTeamRunIndex],
        homerun: row[awayHomerunIndex],
        strikeOut: row[awayStrikedOutIndex],
        walkWithDeadball: row[awayWalkIndex] + row[awayDeadballIndex],
      });
    }
  }

  // For each team, sort games descending by date, keep the latest 10,
  // then reverse the array so the oldest of the 10 comes first.
  var output = {};
  for (var team in teamGames) {
    var games = teamGames[team];
    games.sort(function (a, b) {
      return b.date - a.date;
    });
    var latestGames = games.slice(0, 10).reverse();
    output[team] = latestGames;
  }

  // ---------------------------
  // Map the data to the target sheet (近十場)
  // ---------------------------

  // Get the desired order and format settings from the 設定 sheet.
  var settingsOrder = [
    settingSheet.getRange("C4").getValue(),
    settingSheet.getRange("C6").getValue(),
    settingSheet.getRange("K4").getValue(),
    settingSheet.getRange("K6").getValue(),
    settingSheet.getRange("S4").getValue(),
    settingSheet.getRange("S6").getValue(),
  ];
  var settingCells = ["C4", "C6", "K4", "K6", "S4", "S6"];

  // Define the six block positions for mapping.
  // B3 (col 2, row 3), B16 (col 2, row 16), O3 (col 15, row 3), O16 (col 15, row 16), AB3 (col 28, row 3), AB16 (col 28, row 16)
  var blockPositions = [
    { row: 3, col: 2 },
    { row: 16, col: 2 },
    { row: 3, col: 15 },
    { row: 16, col: 15 },
    { row: 3, col: 28 },
    { row: 16, col: 28 },
  ];

  // Define the field order for each game record.
  var fieldOrder = [
    "date",
    "opponent",
    "opposingPitcher",
    "park",
    "realPointsEarned",
    "pointsEarned",
    "pointsLost",
    "realPointsLost",
    "hits",
    "strikeOut",
    "walkWithDeadball",
    "homerun",
  ];

  // Loop through each team (based on settingsOrder)
  for (var i = 0; i < settingsOrder.length; i++) {
    // fullTeamName from settingsOrder is used to find a match.
    var fullTeamName = settingsOrder[i];
    var matchedKey = null;
    // Use "include" check: if the fullTeamName contains the abbreviated key.
    for (var key in output) {
      if (fullTeamName.indexOf(key) !== -1) {
        matchedKey = key;
        break;
      }
    }
    // Get the corresponding block position.
    var block = blockPositions[i];
    if (matchedKey) {
      // Use transformText to get the header display value from the matched key.
      var displayName = transformText(matchedKey);
      targetSheet.getRange(block.row, block.col).setValue(displayName);

      // Set the header background and font colors from the 設定 sheet.
      var settingRange = settingSheet.getRange(settingCells[i]);
      var bgColor = settingRange.getBackground();
      var fontColor = settingRange.getFontColor();
      // The header range spans 12 columns (e.g. B3:M3)
      var headerRange = targetSheet.getRange(block.row, block.col, 1, 12);
      headerRange.setBackground(bgColor);
      headerRange.setFontColor(fontColor);

      // Retrieve the latest 10 games for the matched team.
      var games = output[matchedKey];
      // Prepare a 10x12 array for game data.
      var gameData = [];
      for (var j = 0; j < 10; j++) {
        var rowData = [];
        if (j < games.length) {
          var game = games[j];
          for (var f = 0; f < fieldOrder.length; f++) {
            var cellValue = game[fieldOrder[f]];
            // Apply transformation to game data cell values if they are strings.
            cellValue = transformText(cellValue);
            rowData.push(cellValue);
          }
        } else {
          rowData = new Array(12).fill("");
        }
        gameData.push(rowData);
      }
      // Write the game data starting one row below the header.
      targetSheet
        .getRange(block.row + 1, block.col, 10, 12)
        .setValues(gameData);
    }
  }

  console.log(output);
}
