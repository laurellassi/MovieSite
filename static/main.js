// ELEMENT REFERENCES
const navbar = document.getElementById("navbar");
const hamburger = document.getElementById("hamburger");
const dropdownMenu = document.getElementById("dropdownMenu");
const exploreBtn = document.getElementById("exploreBtn");

// HAMBURGER MENU TOGGLE
function toggleHamburger(e) {
    // Prevent click from bubbling up to document
    e.stopPropagation();
    
    // Toggle active class and get current state
    const isActive = hamburger.classList.toggle("active");

    // Apply same state to dropdown menu
    dropdownMenu.classList.toggle("active", isActive);

    // Update accessibility attribute
    hamburger.setAttribute("aria-expanded", String(isActive));
}

// Open/close menu when hamburger is clicked
hamburger.addEventListener("click", toggleHamburger);

// Close menu when clicking outside
document.addEventListener("click", (e) => {
    if (!dropdownMenu.contains(e.target) && !hamburger.contains(e.target)) {
        dropdownMenu.classList.remove("active");
        hamburger.classList.remove("active");
        hamburger.setAttribute("aria-expanded", "false");
    }
});

// EXPLORE BUTTON SMOOTH SCROLL
exploreBtn.addEventListener("click", () => {
    // Find the movies section
    const moviesSection = document.querySelector(".movies-section");

    // Scroll to it smoothly if it exists
    if (moviesSection) {
        moviesSection.scrollIntoView({ behavior: "smooth" });
    }
});

// INTERSECTION OBSERVER
// Add 'visible' class when sections come into view
const observer = new IntersectionObserver(
    (entries) => {
        entries.forEach((entry) => {
            if (entry.isIntersecting) entry.target.classList.add("visible");
        });
    },
    { threshold: 0.15 } // Trigger when 15% visible
);

// Observe all movie section divs
document
    .querySelectorAll(".movies-section > div")
    .forEach((section) => observer.observe(section));

// CAROUSEL BUTTON LOGIC
document.querySelectorAll(".carousel-container").forEach((container) => {
    // Get carousel elements
    const viewport = container.querySelector(".carousel-viewport");
    const track = container.querySelector(".carousel-track");
    const leftBtn = container.querySelector(".carousel-btn.left");
    const rightBtn = container.querySelector(".carousel-btn.right");

    // Skip if any element is missing
    if (!viewport || !track || !leftBtn || !rightBtn) return;

    // Track current scroll position
    let currentX = 0;

    // Calculate maximum scroll distance
    function maxScroll() {
        return Math.max(0, track.scrollWidth - viewport.clientWidth);
    }

    // Keep value within min/max bounds
    function clamp(value, min, max) {
        return Math.min(Math.max(value, min), max);
    }

    // Move carousel left or right
    function move(direction) {
        const card = track.querySelector(".movie-item");
        if (!card) return;

        // Calculate distance to move (card width + gap)
        const gap = parseFloat(getComputedStyle(track).gap) || 0;
        const step = card.offsetWidth + gap;

        // Update position
        currentX += direction * step;
        currentX = clamp(currentX, 0, maxScroll());

        // Apply transform
        track.style.transform = `translateX(-${currentX}px)`;

        // Update button states
        updateButtons();
    }

    // Enable/disable buttons based on scroll position
    function updateButtons() {
        const max = maxScroll();

        // Disable left button at start
        if (currentX <= 0) {
            leftBtn.classList.add("is-disabled");
        } else {
            leftBtn.classList.remove("is-disabled");
        }

        // Disable right button at end
        if (currentX >= max) {
            rightBtn.classList.add("is-disabled");
        } else {
            rightBtn.classList.remove("is-disabled");
        }
    }

    // Attach button click handlers
    leftBtn.addEventListener("click", () => move(-1));
    rightBtn.addEventListener("click", () => move(1));

    // Recalculate on window resize
    window.addEventListener("resize", () => {
        currentX = clamp(currentX, 0, maxScroll());
        track.style.transform = `translateX(-${currentX}px)`;
        updateButtons();
    });

    // Initialize button states
    updateButtons();
});

// RESET CAROUSELS ON RESIZE
// Reset all carousels to starting position when window is resized
window.addEventListener("resize", () => {
    document.querySelectorAll(".carousel-track").forEach((track) => {
        track.style.transform = "translateX(0)";
    });
});
