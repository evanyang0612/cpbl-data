function mapLatestGamesByDate() {
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

  // Helper to parse rows from a sheet into teamGames object
  function parseSheetIntoTeamGames(sheet, teamGames, currentYear) {
    var data = sheet.getDataRange().getValues();
    var headers = data[0];

    var homeTeamIndex = headers.indexOf("主隊");
    var awayTeamIndex = headers.indexOf("客隊");
    var homePitcherIndex = headers.indexOf("主隊先發");
    var awayPitcherIndex = headers.indexOf("客隊先發");
    var dateIndex = headers.indexOf("日期");
    var parkIndex = headers.indexOf("球場");
    var homeTeamEarnedRunIndex = headers.indexOf("主總自責分");
    var awayTeamEarnedRunIndex = headers.indexOf("客總自責分");
    var homeTeamRunIndex = headers.indexOf("主得分");
    var awayTeamRunIndex = headers.indexOf("客得分");
    var homeHitsIndex = headers.indexOf("主總安打");
    var awayHitsIndex = headers.indexOf("客總安打");
    var homeHomerunIndex = headers.indexOf("主全壘打");
    var awayHomerunIndex = headers.indexOf("客全壘打");
    var homeWalkIndex = headers.indexOf("主四壞球");
    var homeDeadballIndex = headers.indexOf("主死球");
    var awayWalkIndex = headers.indexOf("客四壞球");
    var awayDeadballIndex = headers.indexOf("客死球");
    var homeStrikedOutIndex = headers.indexOf("主被三振");
    var awayStrikedOutIndex = headers.indexOf("客被三振");

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
        "One or more required headers not found in sheet: " + sheet.getName(),
      );
    }

    for (var i = 1; i < data.length; i++) {
      var row = data[i];
      var gameDate = new Date(row[dateIndex]);
      if (gameDate.getFullYear() !== currentYear) continue;

      var homeTeam = row[homeTeamIndex];
      if (homeTeam) {
        if (!teamGames[homeTeam]) teamGames[homeTeam] = [];
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

      var awayTeam = row[awayTeamIndex];
      if (awayTeam) {
        if (!teamGames[awayTeam]) teamGames[awayTeam] = [];
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
  }

  // Open the spreadsheet and get sheets.
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var targetSheet = ss.getSheetByName("近十場");
  var settingSheet = ss.getSheetByName("設定");
  var currentYear = new Date().getFullYear();

  // Collect games from both preseason (熱身賽賽程) and regular season (賽程).
  var teamGames = {};
  parseSheetIntoTeamGames(ss.getSheetByName("熱身賽賽程"), teamGames, currentYear);
  parseSheetIntoTeamGames(ss.getSheetByName("賽程"), teamGames, currentYear);

  // For each team, sort games descending by date, keep the latest 10,
  // then reverse so the oldest of the 10 comes first.
  var output = {};
  for (var team in teamGames) {
    var games = teamGames[team];
    games.sort(function (a, b) {
      return b.date - a.date;
    });
    output[team] = games.slice(0, 10).reverse();
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

  var blockPositions = [
    { row: 3, col: 2 },
    { row: 16, col: 2 },
    { row: 3, col: 15 },
    { row: 16, col: 15 },
    { row: 3, col: 28 },
    { row: 16, col: 28 },
  ];

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

  for (var i = 0; i < settingsOrder.length; i++) {
    var fullTeamName = settingsOrder[i];
    var matchedKey = null;
    for (var key in output) {
      if (fullTeamName.indexOf(key) !== -1) {
        matchedKey = key;
        break;
      }
    }
    var block = blockPositions[i];
    if (matchedKey) {
      var displayName = transformText(matchedKey);
      targetSheet.getRange(block.row, block.col).setValue(displayName);

      var settingRange = settingSheet.getRange(settingCells[i]);
      var bgColor = settingRange.getBackground();
      var fontColor = settingRange.getFontColor();
      var headerRange = targetSheet.getRange(block.row, block.col, 1, 12);
      headerRange.setBackground(bgColor);
      headerRange.setFontColor(fontColor);

      var games = output[matchedKey];
      var gameData = [];
      for (var j = 0; j < 10; j++) {
        var rowData = [];
        if (j < games.length) {
          var game = games[j];
          for (var f = 0; f < fieldOrder.length; f++) {
            rowData.push(transformText(game[fieldOrder[f]]));
          }
        } else {
          rowData = new Array(12).fill("");
        }
        gameData.push(rowData);
      }
      targetSheet
        .getRange(block.row + 1, block.col, 10, 12)
        .setValues(gameData);
    }
  }

  console.log(output);
}
