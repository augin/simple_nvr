// клик по камере
function changeCamera(camera) {
  const video = document.getElementById('video');
  const hvideo = document.getElementById('cam-name');
  hvideo.innerHTML = `${camera}`;
  fetch_files(`${camera}`);

}

// Глобальная переменная для хранения текущей скорости воспроизведения
let currentPlaybackSpeed = 1;

// Функция для изменения скорости воспроизведения видео
function changePlaybackSpeed(speed) {
  const video = document.getElementById('video');
  video.playbackRate = speed;

  // Сохраняем текущую скорость в глобальную переменную
  currentPlaybackSpeed = speed;

  // Удалите класс active у всех кнопок скорости воспроизведения
  const speedButtons = document.querySelectorAll('#speed-buttons button');
  speedButtons.forEach(button => {
    button.classList.remove('active');
  });

  // Добавьте класс active к текущей кнопке скорости воспроизведения
  const currentSpeedButton = document.querySelector(`#speed-buttons button[data-speed="${speed}"]`);
  if (currentSpeedButton) {
    currentSpeedButton.classList.add('active');
  }
}

// Функция для включения нового видео с сохраненной скоростью воспроизведения
function playNewVideo(videoPath) {
  const video = document.getElementById('video');
  video.src = videoPath;
  video.load();
  video.playbackRate = currentPlaybackSpeed; // Устанавливаем сохраненную скорость
  video.play();
}


// Запрос списка камер с сервера
async function fetchCameraList() {
  try {
    const response = await fetch('cams.php');
    const data = await response.json();

    const cameraButtons = document.getElementById('camera-buttons');

    // Создание кнопок для каждой камеры
    data.cameras.forEach(camera => {
      const button = document.createElement('button');
      button.textContent = `${camera}`;
      button.onclick = () => changeCamera(camera);
      const listItem = document.createElement('li');
      listItem.appendChild(button);
      cameraButtons.appendChild(listItem);
    });
  } catch (error) {
    console.error('Ошибка при получении списка камер:', error);
  }
}

// Добавление обработчика событий для элементов списка камер
function addCameraButtonClickHandlers() {
  const cameraButtons = document.querySelectorAll('#camera-buttons button');
  cameraButtons.forEach(button => {
    button.addEventListener('click', () => {
      // Удаляем класс active у всех кнопок
      cameraButtons.forEach(btn => {
        btn.classList.remove('active');
      });
      // Добавляем класс active к нажатой кнопке
      button.classList.add('active');
    });
  });
}

// Вызов функции добавления обработчиков событий после загрузки списка камер
fetchCameraList().then(() => {
  addCameraButtonClickHandlers();
});

// Clear existing file tree
function clearFileTree(parent) {
    while (parent.firstChild) {
        parent.removeChild(parent.firstChild);
    }
}

function generateFileTree(data, parent) {
    // Clear existing file tree
    clearFileTree(parent);
    // Iterate through data and generate file tree
    Object.keys(data).forEach(key => {
        const folderLi = document.createElement('li');
        const folderSpan = document.createElement('span');
        const campath = "video/" + document.getElementById("cam-name").innerHTML + "/";
        folderSpan.textContent = key.replace(campath, '');
        folderSpan.classList.add('folder');
        folderLi.appendChild(folderSpan);
        const ul = document.createElement('ul');

        ul.classList.add('collapsed'); // Add collapsed class by default

        data[key].forEach(file => {
            const fileLi = document.createElement('li');
            const fileTextNode = document.createTextNode(file.replace(".mp4", ''));
            fileLi.appendChild(fileTextNode);
            ul.appendChild(fileLi);
        });

        folderLi.appendChild(ul);
        parent.appendChild(folderLi);

        // Add click event listener to toggle files visibility
        folderSpan.addEventListener('click', () => {
            ul.classList.toggle('collapsed');
        });
    });
}

// Fetch JSON data from
function fetch_files(camera) {
    fetch(`files.php?camera=${camera}`)
      .then(response => response.json())
      .then(data => {
        const folderTreeContainer = document.getElementById("folderTree");

        // Вызов функции добавления обработчиков событий после генерации дерева файлов
        generateFileTree(data, folderTreeContainer);
        addFileClickHandlers();

        playSecondFileDefault();

        // Автоматическое раскрытие первого элемента дерева
        const firstFolderSpan = folderTreeContainer.querySelector('.folder');
        if (firstFolderSpan) {
            const ul = firstFolderSpan.nextElementSibling;
            ul.classList.remove('collapsed');
       }

      })
      .catch(error => console.error('Error fetching data:', error));
}


function addFileClickHandlers() {
  const fileElements = document.querySelectorAll('#folderTree li ul li');
  fileElements.forEach((fileElement, index) => {
    fileElement.addEventListener('click', () => {
      const camera = document.getElementById('cam-name').innerHTML;
      const fileName = fileElement.textContent;

      // Удалите класс active у всех элементов списка файлов
      fileElements.forEach(file => {
        file.classList.remove('active');
      });
      // Добавьте класс active к проигрываемому файлу
      fileElement.classList.add('active');

      // Получите путь к видео из дерева файлов
      const folderSpan = fileElement.parentElement.parentElement.firstChild;
      const folderName = folderSpan.textContent;
      const campath = "video/" + document.getElementById("cam-name").innerHTML + "/";
      const videoPath = `${campath}${folderName}/${fileName}.mp4`;

      // Воспроизведение нового видео с сохраненной скоростью
      playNewVideo(videoPath);
    });
  });
}

const video = document.getElementById('video');

// Добавление обработчика события "ended" к элементу видео
video.addEventListener('ended', () => {
  const activeFileElement = document.querySelector('#folderTree li ul li.active');
  if (activeFileElement) {
    const previousFileElement = activeFileElement.previousElementSibling;
    if (previousFileElement) {
      // Воспроизведение предыдущего файла
      previousFileElement.click();
    } else {
      // Если предыдущего файла нет, можно воспроизвести последний файл
      const lastFileElement = document.querySelector('#folderTree li ul li:last-child');
      if (lastFileElement) {
        lastFileElement.click();
      }
    }
  }
});


// Функция для воспроизведения второго файла по умолчанию
function playSecondFileDefault() {
  const secondFileElement = document.querySelector('#folderTree li ul li:nth-child(2)');
  if (secondFileElement) {
    secondFileElement.click();
  }
}
