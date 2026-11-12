const navigation = document.querySelector(".navigation");
const narrow = window.matchMedia("(max-width: 720px)");
function setNavigation() {
  navigation.open = !narrow.matches;
}
setNavigation();
narrow.addEventListener("change", setNavigation);
